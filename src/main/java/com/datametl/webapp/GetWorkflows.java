package com.datametl.webapp;

import com.datametl.jobcontrol.Job;
import com.datametl.jobcontrol.JobState;
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

/**
 * Servlet implementation class FileCounter
 */
@WebServlet("/getworkflows")
public class GetWorkflows extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        Map<String, UUID> namedJobs = Index.manager.getNamedJobs();
        JSONObject statusJson = new JSONObject();

        String requestedStatus = request.getParameter("status");
        String requestedJob = request.getParameter("jobid");

        JobState state = null;
        PrintWriter out = response.getWriter();

        if(requestedStatus != null) {
            if (requestedStatus.equals("RUNNING")) {
                state = JobState.RUNNING;
            } else if (requestedStatus.equals("SUCCESS")) {
                state = JobState.SUCCESS;
            } else if (requestedStatus.equals("FAILED")) {
                state = JobState.FAILED;
            } else {
                response.setStatus(400);
                out.println("{\"error\": \"Invalid status check.\"}");
                return;
            }
            for(String name: namedJobs.keySet()) {
                if (state == JobState.FAILED) {
                    if (Index.manager.getJobByName(name).getState() == state || Index.manager.getJobByName(name).getState() == JobState.KILLED) {
                        JSONObject obj = new JSONObject();

                        obj.put("status", Index.manager.getJobByName(name).getState());
                        statusJson.put(name, obj);
                    }
                } else {
                    if (Index.manager.getJobByName(name).getState() == state) {
                        JSONObject obj = new JSONObject();
                        obj.put("status", Index.manager.getJobByName(name).getState());
                        statusJson.put(name, obj);
                    }
                }
            }
            out.println(statusJson.toString());
            return;
        } else if (requestedJob != null) {
            out.println(Index.manager.getJobByName(requestedJob).getETLPacket().toString());
            return;
        } else {
            response.setStatus(400);
            out.println("{\"error\": \"Invalid status check.\"}");
            return;
        }
    }


    @Override
    public void init() throws ServletException {
        super.init();
    }

    public void destroy() {
        super.destroy();
    }

}