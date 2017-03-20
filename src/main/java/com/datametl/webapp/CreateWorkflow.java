package com.datametl.webapp;

import com.datametl.jobcontrol.Job;
import com.datametl.jobcontrol.SubJob;
import com.datametl.tasks.DataSegmentationTask;
import com.datametl.tasks.Task;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

/**
 * Servlet for createworkflow
 */
@WebServlet("/createworkflow")
public class CreateWorkflow extends HttpServlet {
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
                "\t\t\"host_ip\": \"\",\n" +
                "\t\t\"host_port\": \"\",\n" +
                "\t\t\"path\": \"\",\n" +
                "\t\t\"file_type\": \"\"\n" +
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
                "\t\t\"host_ip\": \"\",\n" +
                "\t\t\"host_port\": \"\",\n" +
                "\t\t\"username\": \"\",\n" +
                "\t\t\"password\": \"\",\n" +
                "\t\t\"storage_type\": \"\"\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"source_header\": \"\",\n" +
                "\t\t\"destination_header\": \"\",\n" +
                "\t\t\"contents\": [],\n" +
                "\t}\n" +
                "}";
        JSONObject etlPacket = new JSONObject(emptyPacketData);
        etlPacket.getJSONObject("source").put("path", requestParams.get("source")[0]);
        etlPacket.getJSONObject("source").put("file_type", requestParams.get("source_type")[0]);

        etlPacket.getJSONObject("destination").put("host_ip", requestParams.get("destination_ip")[0]);
        etlPacket.getJSONObject("destination").put("host_port", requestParams.get("destination_port")[0]);
        etlPacket.getJSONObject("destination").put("username", requestParams.get("username")[0]);
        etlPacket.getJSONObject("destination").put("password", requestParams.get("password")[0]);
        etlPacket.getJSONObject("destination").put("storage_type", requestParams.get("destination_type")[0]);

        //TODO: Get the destination header

        UUID id = Index.manager.addJob(etlPacket);
        Index.manager.startJob(id);
    }


    @Override
    public void init() throws ServletException {
        super.init();
    }

    public void destroy() {
        super.destroy();
    }

}