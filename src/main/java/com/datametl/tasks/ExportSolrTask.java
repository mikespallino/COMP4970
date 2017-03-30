  package com.datametl.tasks;
import java.lang.Object;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
//import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.imageio.IIOException;
import java.net.MalformedURLException;
import java.util.ArrayList;



public class ExportSolrTask implements ExportInterface, Task {
    private SolrClient conn;
    private String host_address;
    private SolrInputDocument doc = new SolrInputDocument();
    private ArrayList<String> columns = new ArrayList<String>();
    private StringBuilder statement;
    private JobState state;
    private SubJob parent;

    public void initiateConnection() {SolrClient conn = new HttpSolrClient(host_address); }

    public void terminateConnection() {
        try {
            conn.close();
        }
        catch (java.io.IOException e){
            e.printStackTrace();
        }
    }
    public void retrieveContents(JSONObject packet) {
        JSONObject connInfo = packet.getJSONObject("destination");
        JSONArray content = packet.getJSONArray("contents");
        JSONArray headers = packet.getJSONArray("destination_header");

        host_address = connInfo.getString("host_ip");
        host_address.concat(":");
        host_address.concat(connInfo.getString("host_port"));


        for (Object j: headers){
            if(j instanceof String){
                columns.add("\"" + (j.toString() + "\""));
            }
        }
        int x = 0;
        for (Object i : content) {
            doc.addField(columns.get(x), ("\"" + (i.toString()) + "\""));
            if(x == columns.size()){
                x = 0;
            }
            else{
                x++;
            }
        }
    }

    public void exportToDSS() {
        try {
            conn.add(doc);
            conn.commit();
        }
        catch(MalformedURLException e){
            e.printStackTrace();
        }
        catch(java.io.IOException e){
            e.printStackTrace();
        }
        catch(SolrServerException e){
            e.printStackTrace();
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