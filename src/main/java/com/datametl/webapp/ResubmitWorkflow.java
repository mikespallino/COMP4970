package com.datametl.webapp;

import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mspallino on 4/6/17.
 */

@WebServlet("/resubmit")
public class ResubmitWorkflow extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doPost(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/plain");
        PrintWriter out = response.getWriter();
        Map<String, String[]> requestParams = request.getParameterMap();
        out.println("Request Parameters:\n\n");
        for(String key: requestParams.keySet()) {
            out.print(key + ": \n");
            String[] val = requestParams.get(key);
            for (int i = 0; i < val.length; ++i) {
                out.print(val[i] + ", ");
            }
            out.println("");
        }


        String emptyPacketData = "{\n" +
                "\t\"source\": {\n" +
                "\t\t\"host_ip\": null,\n" +
                "\t\t\"host_port\": null,\n" +
                "\t\t\"path\": null,\n" +
                "\t\t\"file_type\": null\n" +
                "\t},\n" +
                "\t\"rules\": {\n" +
                "\t\t\"transformations\": {\n" +
                "\t\t},\n" +
                "\t\t\"mappings\": {\n" +
                "\t\t},\n" +
                "\t\t\"filters\": {\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"destination\": {\n" +
                "\t\t\"host_ip\": null,\n" +
                "\t\t\"host_port\": null,\n" +
                "\t\t\"username\": null,\n" +
                "\t\t\"password\": null,\n" +
                "\t\t\"storage_type\": null\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"source_header\": \"\",\n" +
                "\t\t\"destination_header\": null,\n" +
                "\t\t\"contents\": [],\n" +
                "\t}\n" +
                "}";
        Map<String, Object> newRequestParams = new HashMap<String, Object>();
        for(String key: requestParams.keySet()) {
            Object item = requestParams.get(key)[0];
            if (item.equals("")) {
                item = JSONObject.NULL;
            }
            newRequestParams.put(key, item);
        }
        JSONObject etlPacket = new JSONObject(emptyPacketData);
        etlPacket.getJSONObject("source").put("path", newRequestParams.get("source"));
        etlPacket.getJSONObject("source").put("file_type", newRequestParams.get("source_type"));

        etlPacket.getJSONObject("destination").put("host_ip", newRequestParams.get("destination_ip"));
        etlPacket.getJSONObject("destination").put("host_port", newRequestParams.get("destination_port"));
        etlPacket.getJSONObject("destination").put("username", newRequestParams.get("username"));
        etlPacket.getJSONObject("destination").put("password", newRequestParams.get("password"));
        etlPacket.getJSONObject("destination").put("storage_type", newRequestParams.get("destination_type"));
        etlPacket.getJSONObject("destination").put("destination_location", newRequestParams.get("destination_location"));
        JSONArray destinationHeader = new JSONArray();
        Object objHeader = newRequestParams.get("destination_schema");
        String header;
        if (objHeader != JSONObject.NULL) {
            header = (String) objHeader;
            for(String s: header.split(",")) {
                destinationHeader.put(s);
            }
        } else {
            destinationHeader.put(objHeader);
        }
        etlPacket.getJSONObject("data").put("destination_header", destinationHeader);

        //TODO: This isn't really good. It won't work if the user creates and deletes a lot in the UI
        int transformCount = 1;
        int filterCount = 1;

        for (int i = 0; i < 300; i++) {
            if (requestParams.containsKey("transformSourceField" + i)) {
                JSONObject transform = new JSONObject();
                transform.put("source_column", newRequestParams.get("transformSourceField" + i));
                transform.put("new_field", newRequestParams.get("transformDestinationField" + i));
                transform.put("transform", ((String) newRequestParams.get("transformValueComp" + i)).toUpperCase() + " " + newRequestParams.get("transformValue" + i));
                etlPacket.getJSONObject("rules").getJSONObject("transformations").put("transform" + transformCount, transform);

                transformCount++;
            }
            if (requestParams.containsKey("mappingSourceField" + i)) {
                if (newRequestParams.get("mappingSourceField" + i) != JSONObject.NULL) {
                    etlPacket.getJSONObject("rules").getJSONObject("mappings").put((String) newRequestParams.get("mappingSourceField" + i), newRequestParams.get("mappingDestinationField" + i));
                }
            }
            if (requestParams.containsKey("filterSourceField" + i)) {
                JSONObject filter = new JSONObject();
                filter.put("source_column", newRequestParams.get("filterSourceField" + i));
                filter.put("equality_test", ((String) newRequestParams.get("filterValueComp" + i)).toUpperCase());
                filter.put("filter_value", newRequestParams.get("filterValue" + i));
                etlPacket.getJSONObject("rules").getJSONObject("filters").put("filter" + filterCount, filter);
                filterCount++;
            }
        }

        String name = requestParams.get("name")[0];
        //INFO: We can't have spaces in names
        name = name.replace(" ", "_");
        etlPacket.put("schedule", requestParams.get("schedule")[0]);
        etlPacket.put("time", requestParams.get("time")[0]);
        etlPacket.put("name", name);

        UUID id = Index.manager.resubmitJob(name, etlPacket);
        if (id == null) {
            response.sendError(400, "Job name already in use!");
            return;
        }
        Index.manager.startJob(id);
    }
}
