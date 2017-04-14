package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.*;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class ExportElasticSearchTask implements ExportInterface, Task {
    private String host_address;
    private int host_port;
    private Client client;
    private String cluster;
    private String index;
    private String type;
    private JobState state;
    private SubJob parent;
    private JSONObject etlPacket;
    private ArrayList<JSONObject> book = new ArrayList<JSONObject>();
    private Logger log;

    /**
     * Constructor
     * <p>
     * Sets JobState to NOT_STARTED
     */
    public ExportElasticSearchTask() {
        state = JobState.NOT_STARTED;
    }

    /**
     * Establishes a connection to the Elastic Search data storage system.
     * <p>
     * It requires that the etlPacket is not null.
     * It retrieves the host_ip and stores it to the host_address variable.
     * Then it retrieves the host_port and stores it to host_port.
     * Then it creates an array from destination_location containing information
     * for cluster, index, type.
     * It creates a TransportClient object using host_address and host_port for
     * later use with communicating with the Elastic Search data storage system.
     */
    public void initiateConnection() {
        JSONObject connInfo = etlPacket.getJSONObject("destination");
        host_address = connInfo.getString("host_ip");
        host_port = connInfo.getInt("host_port");
        String[] tmp = connInfo.getString("destination_location").split("\\.");
        cluster = tmp[0];
        index = tmp[1];
        type = tmp[2];


        Settings settings = Settings.builder().put("cluster.name", cluster).build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host_address), host_port));
        } catch (UnknownHostException ex) {
            log.error(ex.getMessage());
            throw new RuntimeException("Failed to get connection.");
        }
    }

    /**
     * Ends connection to Elastic Search data storage system.
     * It requires that a client object has successfully been
     * successfully created by initiateConnection.
     */
    public void terminateConnection() {
        client.close();
    }

    /**
     * Extracts the data to be stored and the necessary headers for
     * inserting into the correct fields.
     * After the the data and headers are retrieved a doc is built
     * and then added to a book that will later be pushed to the
     * Elastic Search data storage system.
     *
     * @param packet contains data and headers
     */
    public void retrieveContents(JSONObject packet) {
        JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
        JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");


        for (int i = 0; i < content.length(); i++) {
            JSONArray data = content.getJSONArray(i);
            JSONObject doc = new JSONObject();
            for (int j = 0; j < data.length(); j++) {
                doc.put(headers.getString(j), data.get(j));
            }
            book.add(doc);
        }

    }

    /**
     * Iterates through the book created in the retrieveContents method
     * and creates a response using each indice as well a inex and type
     * from the initiateConnection method.
     * Each response is sent upon instantiation.
     */
    public void exportToDSS() {
        for (JSONObject i : book) {
            IndexResponse response = client.prepareIndex(index, type)
                    .setSource(i.toString())
                    .execute()
                    .actionGet();
        }
    }

    /**
     * Stores packet information to the etlPacket variable.
     * Then it calls initiate connection.
     * Then it calls retrieveContents.
     * Then it calls exportToDSS.
     * Then it calls terminateConnection.
     */
    public void apply() {
        state = JobState.RUNNING;

        etlPacket = parent.getETLPacket();
        log.info("Connecting...");
        initiateConnection();
        log.info("Parsing contents...");
        retrieveContents(etlPacket);
        log.info("Exporting...");
        exportToDSS();
        log.info("Closing");
        terminateConnection();

        state = JobState.SUCCESS;
    }

    /**
     * Returns the state of this job.
     *
     * @return
     */
    public JobState getResult() {
        return state;
    }

    /**
     * Sets the parent of this Task.
     *
     * @param parent
     */
    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    /**
     * Returns the parent of this Task.
     *
     * @return
     */
    public SubJob getParent() {
        return parent;
    }

    /**
     * Sets the logger for this task to use
     * @param log
     */
    @Override
    public void setLogger(Logger log) {
        this.log = log;
    }
}