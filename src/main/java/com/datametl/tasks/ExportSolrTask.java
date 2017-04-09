  package com.datametl.tasks;
import java.lang.Object;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.imageio.IIOException;
import java.net.MalformedURLException;
import java.util.ArrayList;



public class ExportSolrTask implements ExportInterface, Task {
    private SolrClient conn;
    private String host_address = "http://";
    private SolrInputDocument doc = new SolrInputDocument();
    private JobState state;
    private SubJob parent;
    private JSONObject etlPacket;
    private Logger log;


    public ExportSolrTask(){
        state = JobState.NOT_STARTED;
    }

    public void initiateConnection() {
        JSONObject connInfo = etlPacket.getJSONObject("destination");
        host_address += connInfo.getString("host_ip");
        host_address += ":" + String.valueOf(connInfo.getInt("host_port"));
        host_address += "/solr";
        host_address += "/" + connInfo.getString("destination_location");

        conn = new HttpSolrClient(host_address);
    }

    public void terminateConnection() {
        try {
            conn.close();
        }
        catch (java.io.IOException e){
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
    public void retrieveContents(JSONObject packet) {
        JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
        JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");

        for(int i = 0; i < content.length(); i++) {
            JSONArray data = content.getJSONArray(i);
            for(int j = 0; j < data.length(); j++){
                doc.addField(headers.getString(j), data.get(j));
            }
            try {
                conn.add(doc);
                doc = new SolrInputDocument();
            }
            catch(MalformedURLException e){
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
            catch(java.io.IOException e){
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
            catch(SolrServerException e){
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public void exportToDSS() {
        try {
            conn.commit();
        }
        catch(MalformedURLException e){
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        catch(java.io.IOException e){
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        catch(SolrServerException e){
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }

    }

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