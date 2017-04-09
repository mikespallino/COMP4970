  package com.datametl.tasks;

  import com.datametl.jobcontrol.JobState;
  import com.datametl.jobcontrol.SubJob;
  import com.datametl.logging.Logger;
  import org.elasticsearch.action.index.IndexResponse;
  import org.elasticsearch.client.Client;
  import org.elasticsearch.client.transport.TransportClient;
  import org.elasticsearch.common.settings.Settings;
  import org.elasticsearch.common.transport.*;
  import org.elasticsearch.transport.client.PreBuiltTransportClient;
  import org.json.JSONArray;
  import org.json.JSONObject;

  import java.net.InetAddress;
  import java.net.UnknownHostException;
  import java.util.ArrayList;

  import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

//import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;



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

      public ExportElasticSearchTask() {
          state = JobState.NOT_STARTED;
      }


      public void initiateConnection() {
          JSONObject connInfo = etlPacket.getJSONObject("destination");
          host_address = connInfo.getString("host_ip");
          host_port = connInfo.getInt("host_port");
          String[] tmp = connInfo.getString("destination_location").split(".");
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

      public void terminateConnection() {
          client.close();
      }

      public void retrieveContents(JSONObject packet) {
          JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
          JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");


          for(int i = 0; i < content.length(); i++) {
              JSONArray data = content.getJSONArray(i);
              JSONObject doc = new JSONObject();
              for(int j = 0; j < data.length(); j++){
                  doc.put(headers.getString(j), data.get(j));
              }
              book.add(doc);
          }

      }

      public void exportToDSS() {
          for(JSONObject i : book) {
              IndexResponse response = client.prepareIndex(index, type)
                      .setSource(i.toString())
                      .execute()
                      .actionGet();
          }
      }


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

      public JobState getResult() {
          return state;
      }

      public void setParent(SubJob parent) {
          this.parent = parent;
      }

      public SubJob getParent() {
          return parent;
      }

    @Override
    public void setLogger(Logger log) {
        this.log = log;
    }
}