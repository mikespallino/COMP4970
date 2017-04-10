package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.MalformedURLException;


public class ExportSolrTask implements ExportInterface, Task {
    private SolrClient conn;
    private String host_address = "http://";
    private SolrInputDocument doc = new SolrInputDocument();
    private JobState state;
    private SubJob parent;
    private JSONObject etlPacket;
    private Logger log;


    /**
     * Constructor
     * <p>
     * Sets JobState to NOT_STARTED
     */
    public ExportSolrTask() {
        state = JobState.NOT_STARTED;
    }

    /**
     * Establishes a connection to the Solr data storage system.
     * <p>
     * It requires that the etlPacket is not null.
     * It retrieves the host_ip and appends it to the host_address variable.
     * Then it retrieves the host_port and appends it to the host_address variable.
     * Then it appends solr to the host_address variable.
     * Then it appends destination_location to host_address.
     * It creates a conn object using host_address for
     * later use with communicating with the Solr data storage system.
     */
    public void initiateConnection() {
        JSONObject connInfo = etlPacket.getJSONObject("destination");
        host_address += connInfo.getString("host_ip");
        host_address += ":" + String.valueOf(connInfo.getInt("host_port"));
        host_address += "/solr";
        host_address += "/" + connInfo.getString("destination_location");

        conn = new HttpSolrClient(host_address);
    }

    /**
     * Ends connection to Solr data storage system.
     * It requires that a conn object has successfully been
     * successfully created by initiateConnection.
     */
    public void terminateConnection() {
        try {
            conn.close();
        } catch (java.io.IOException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the data to be stored and the necessary headers for
     * inserting into the correct fields.
     * After the the data and headers are retrieved a doc is built
     * and then added to the conn object to later be commited to
     * the Solr data storage system.
     *
     * @param packet contains data and headers
     */
    public void retrieveContents(JSONObject packet) {
        JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
        JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");

        for (int i = 0; i < content.length(); i++) {
            JSONArray data = content.getJSONArray(i);
            for (int j = 0; j < data.length(); j++) {
                doc.addField(headers.getString(j), data.get(j));
            }
            try {
                conn.add(doc);
                doc = new SolrInputDocument();
            } catch (MalformedURLException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            } catch (java.io.IOException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            } catch (SolrServerException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Commits the conn object to the Solr data storage system.
     */
    public void exportToDSS() {
        try {
            conn.commit();
        } catch (MalformedURLException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (java.io.IOException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (SolrServerException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
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
        log.info("Parsing...");
        retrieveContents(etlPacket);
        log.info("Exporting...");
        exportToDSS();
        log.info("Closing");
        terminateConnection();

        state = JobState.SUCCESS;
    }

    /**
     * Stores packet information to the etlPacket variable.
     * Then it calls initiate connection.
     * Then it calls retrieveContents.
     * Then it calls exportToDSS.
     * Then it calls terminateConnection.
     */

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