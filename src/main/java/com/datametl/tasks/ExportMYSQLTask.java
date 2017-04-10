package com.datametl.tasks;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ExportMYSQLTask implements ExportInterface, Task {
    private Connection conn;
    private String host_address = "jdbc:mysql://";
    private String username;
    private String password;
    private String table;
    private StringBuilder columns;
    private StringBuilder statement;
    private JobState state;
    private SubJob parent;
    private JSONObject etlPacket;
    private Statement stmt = null;
    private Logger log;

    public ExportMYSQLTask() {
        columns = new StringBuilder();
        statement = new StringBuilder();
        state = JobState.NOT_STARTED;
    }

    public void initiateConnection() {
        JSONObject connInfo = etlPacket.getJSONObject("destination");
        host_address += connInfo.getString("host_ip");
        host_address += ":" + String.valueOf(connInfo.getInt("host_port")) + "?useJDBCCompliantTimezoneShift=true&serverTimezone=UTC";

        username = connInfo.getString("username");
        password = connInfo.getString("password");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(host_address, username, password);
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (ClassNotFoundException ex) {
            log.error(ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    public void terminateConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public void retrieveContents(JSONObject packet) {
        JSONObject connInfo = packet.getJSONObject("destination");
        JSONArray content = packet.getJSONObject("data").getJSONArray("contents");
        JSONArray headers = packet.getJSONObject("data").getJSONArray("destination_header");

        try {
            stmt = conn.createStatement();

            if (connInfo.getString("storage_type").equals("mysql")) {
                table = connInfo.getString("destination_location");

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
                    statement.append(table);
                    statement.append(" (");
                    statement.append(columns.toString());
                    statement.append(") VALUES (");
                    statement.append(dataAsString.substring(1,dataAsString.length()-1));
                    statement.append(");");

                    String t = statement.toString();
                    stmt.addBatch(t);
                    statement = new StringBuilder();
                }

            }
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
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
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public void apply() {
        state = JobState.RUNNING;

        try {
            etlPacket = parent.getETLPacket();
            log.info("Connecting...");
            initiateConnection();
            log.info("Parsing contents...");
            retrieveContents(etlPacket);
            log.info("Exporting...");
            exportToDSS();
            log.info("Closing");
            terminateConnection();
        } catch (Exception ex) {
            log.error(ex.getMessage());
            state = JobState.KILLED;
            parent.kill();
        }

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