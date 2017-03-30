package com.datametl.tasks;

import org.json.JSONObject;


public class ExportDecisionFactory {

    public Task pickExporter(String type) {
        if(type.equals("mysql")) {
            return new ExportMYSQLTask();
        } else if(type.equals("postgresql")) {
            return new ExportPostgreSQLTask();
        } else if(type.equals("solr")) {
            return new ExportSolrTask();
        } else if(type.equals("elasticsearch")) {
            return new ExportElasticSearchTask();
        } else {
            // Add a mapping to this for supporting a new Data Storage System
            return null;
        }
    }
}
