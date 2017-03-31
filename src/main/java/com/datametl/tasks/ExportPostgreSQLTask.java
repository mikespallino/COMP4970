package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;


public class ExportPostgreSQLTask implements ExportInterface, Task {
    private Connection conn;
    private String host_address = "jdbc:postgresql://";
    private String username;
    private String password;
    private String table;
    private StringBuilder columns;
    private StringBuilder statement;
    private JobState state;
    private SubJob parent;
    private JSONObject etlPacket;
    private Statement stmt = null;

    public ExportPostgreSQLTask() {
        columns = new StringBuilder();
        statement = new StringBuilder();
        state = JobState.NOT_STARTED;
    }
    public void initiateConnection() {
        JSONObject connInfo = etlPacket.getJSONObject("destination");
        host_address += connInfo.getString("host_ip");
        table = connInfo.getString("destination_location");
        host_address += ":" + String.valueOf(connInfo.getInt("host_port")) + "/" + table.split("\\.")[0];


        username = connInfo.getString("username");
        password = connInfo.getString("password");
        try {
            conn = DriverManager.getConnection(host_address, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void terminateConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void retrieveContents(JSONObject packet) {
        JSONObject connInfo = packet.getJSONObject("destination");
        JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
        JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");

        try {
            stmt = conn.createStatement();
            String db = table.split("\\.")[1];

            if (connInfo.getString("storage_type").equals("postgresql")) {

                int headerLength = headers.length();

                for(int i = 0; i < headerLength; i++) {
                    columns.append(String.valueOf(headers.get(i)));

                    if (i != headerLength-1) {
                        columns.append(",");
                    }
                }

                for(int i = 0; i < content.length(); i++) {
                    JSONArray data = content.getJSONArray(i);
                    String dataAsString = data.toString();

                    statement.append("INSERT INTO ");
                    statement.append(db);
                    statement.append(" (");
                    statement.append(columns.toString());
                    statement.append(") VALUES (");
                    statement.append(dataAsString.substring(1,dataAsString.length()-1).replace("\"", "'"));
                    statement.append(");");

                    String t = statement.toString();
                    System.out.println(t);
                    stmt.addBatch(t);
                    statement = new StringBuilder();
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void exportToDSS() {
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void apply() {
        state = JobState.RUNNING;

        etlPacket = parent.getETLPacket();
        System.out.println("Connecting...");
        initiateConnection();
        System.out.println("Parsing contents...");
        retrieveContents(etlPacket);
        System.out.println("Exporting...");
        exportToDSS();
        System.out.println("Closing");
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
}
