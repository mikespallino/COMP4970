  package com.datametl.tasks;

  import com.datametl.jobcontrol.JobState;
  import com.datametl.jobcontrol.SubJob;
  import org.elasticsearch.action.index.IndexRequest;
  import org.elasticsearch.action.update.UpdateRequest;
  import org.elasticsearch.client.Client;
  import org.elasticsearch.client.transport.*;
  import org.elasticsearch.common.settings.Settings;
  import org.elasticsearch.common.transport.*;
  import org.elasticsearch.transport.Transport;
  import org.elasticsearch.transport.client.PreBuiltTransportClient;
  import org.json.JSONArray;
  import org.json.JSONObject;

  import java.io.IOException;
  import java.io.InterruptedIOException;
  import java.net.InetAddress;
  import java.net.InetSocketAddress;
  import java.net.UnknownHostException;
  import java.util.ArrayList;
  import java.util.concurrent.ExecutionException;

  import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

//import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;



public class ExportElasticSearchTask implements ExportInterface, Task {
      private String host_address;
      private int host_port;
      private String Cluster_Name;
      private String My_Cluster;
      private Client client;

      private ArrayList<String> columns = new ArrayList<String>();
      private ArrayList<String> Data = new ArrayList<String>();
      private String index;
      private String type;
      private String id;
      private JobState state;
      private SubJob parent;


      public void initiateConnection() {
          Settings settings = Settings.builder().put(Cluster_Name, My_Cluster).build();
          try {
              client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host_address), host_port));
          } catch (UnknownHostException ex) {
              ex.printStackTrace();
          }
      }

      public void terminateConnection() {
          client.close();
      }

      public void retrieveContents(JSONObject packet) {
          JSONObject connInfo = packet.getJSONObject("destination");
          JSONArray content = packet.getJSONArray("contents");
          JSONArray headers = packet.getJSONArray("destination_header");

          Cluster_Name = connInfo.getString("Cluster_Name");
          host_address = connInfo.getString("host_ip");
          //host_address.concat(":");
          host_port = connInfo.getInt("host_port");
          index = connInfo.getString("index");
          type = connInfo.getString("type");
          id = connInfo.getString("id");


          for (Object j : headers) {
              if (j instanceof String) {
                  columns.add("\"" + (j.toString()) + "\"");
              }
          }
          int x = 0;
          for (Object i : content) {
              if (i instanceof String) {
                  Data.add("\"" + (i.toString()) + "\"");
              }
          }

      }

      public void exportToDSS() {
          int col_Iterator = 0;
          for (String t : Data) {
              try {
                  IndexRequest indexRequest = new IndexRequest(index, type, id)
                          .source(jsonBuilder()
                                  .startObject()
                                  .field(columns.get(col_Iterator), t)
                                  .endObject());
              } catch (IOException ex) {

              }
              try {
                  UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(jsonBuilder()
                          .startObject()
                          .field(columns.get(col_Iterator), t)
                          .endObject())
                          .upsert(index);
                  client.update(updateRequest).get();
              } catch (IOException ex) {
                  ex.printStackTrace();
              } catch (InterruptedException ex) {
                  ex.printStackTrace();
              } catch (ExecutionException ex) {
                  ex.printStackTrace();
              }
          }
      }


      public void apply() {
          JSONObject ETLPacket = parent.getETLPacket();
          initiateConnection();
          retrieveContents(ETLPacket);

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
}