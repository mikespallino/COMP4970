package com.datametl.tasks;

import org.json.JSONObject;

public interface ExportInterface {
    void initiateConnection();
    void terminateConnection();
    void retrieveContents(JSONObject packet);
    void exportToDSS();
}
