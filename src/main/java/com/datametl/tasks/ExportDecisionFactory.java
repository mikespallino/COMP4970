package com.datametl.tasks;


import java.util.HashMap;
import java.util.Map;

public class ExportDecisionFactory {

    private static Map<String, Class> exporters = new HashMap<>();

    public ExportDecisionFactory() {
        exporters.put("mysql", ExportMYSQLTask.class);
        exporters.put("postgresql", ExportPostgreSQLTask.class);
        exporters.put("solr", ExportSolrTask.class);
        exporters.put("elasticsearch", ExportElasticSearchTask.class);

    }

    public Task pickExporter(String type) {
        try {
            Class t = exporters.get(type);
            return (Task) t.newInstance();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        } catch (InstantiationException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void addNewExporter(String type, Class newExporter) {
        exporters.put(type, newExporter);
    }
}
