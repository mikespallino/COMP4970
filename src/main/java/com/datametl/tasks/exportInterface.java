package com.datametl.tasks;

import org.json.JSONObject;
/**
 * Created by archon on 2/10/17.
 */
public interface exportInterface {
    void initiateInteraction();
    void terminateInteraction();
    /*
    Depending on the requirements of the DSS this will take what is parsed from the packet.
    It will then assemble the parsed text into a formate that can be used in a given DSS insert/ index method
    */
    void composeStatement(String[] fields, String information);
    // This will actually preform the insertion into the DSS
    void exportToDSS(String statement);
    // May return string still deciding
    // Unsure about input parameters
    void parsePacket(JSONObject packet);
    String[] getFields(JSONObject packet);
    String getInformation(JSONObject packet);

}
