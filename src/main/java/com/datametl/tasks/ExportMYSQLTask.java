package com.datametl.tasks;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;


public class ExportMYSQLTask implements ExportInterface, Task {
    private Connection conn;
    private String host_address;
    private String username;
    private String password;
    private String table;
    private StringBuilder columns;
    private StringBuilder values;
    private StringBuilder statement;
    private JobState state;
    private SubJob parent;

    public void initiateConnection() {
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
        JSONArray content = packet.getJSONArray("contents");
        JSONArray headers = packet.getJSONArray("destination_header");

        if(connInfo.getJSONObject("storage_type").getString("DSS_type").equals("MySQL")){
            table = connInfo.getJSONObject("storage_type").getString("table");

            host_address = connInfo.getString("host_ip");
            host_address.concat(":");
            host_address.concat(connInfo.getString("host_port"));

            username = connInfo.getString("username");
            password = connInfo.getString("password");

            for (Object j: headers){
                if(j instanceof String){
                    columns.append("'");
                    columns.append(j.toString());
                    columns.append("'");
                }
            }

            for (Object i : content) {
                if (i instanceof String) {
                    values.append("'");
                    values.append(i.toString());
                    values.append("'");
                }
            }
        }
    }

    public void exportToDSS() {
        try {
            statement.append("INSERT INTO ");
            statement.append(table);
            statement.append(" ");
            statement.append(columns);
            statement.append(" VALUES ");
            statement.append(values);
            conn.createStatement().execute(String.valueOf(statement));
        } catch (SQLException e) {
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