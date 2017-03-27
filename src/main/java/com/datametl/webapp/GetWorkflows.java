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
        Map<UUID, Job> jobs = Index.manager.getJobs();
        JSONObject statusJson = new JSONObject();

        String requestedStatus = request.getParameter("status");
        String requestedJob = request.getParameter("jobid");

        JobState state = null;
        PrintWriter out = response.getWriter();

        if(requestedStatus != null) {
            System.out.println("Checking status: " + requestedStatus);
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
            for(UUID id: jobs.keySet()) {
                if (state == JobState.FAILED) {
                    if (jobs.get(id).getState() == state || jobs.get(id).getState() == JobState.KILLED) {
                        JSONObject obj = new JSONObject();

                        obj.put("status", jobs.get(id).getState());
                        statusJson.put(id.toString(), obj);
                    }
                } else {
                    if (jobs.get(id).getState() == state) {
                        JSONObject obj = new JSONObject();
                        obj.put("status", jobs.get(id).getState());
                        statusJson.put(id.toString(), obj);
                    }
                }
            }
            out.println(statusJson.toString());
        } else if (requestedJob != null) {
            UUID jobId = UUID.fromString(requestedJob);
            out.println(jobs.get(jobId).getETLPacket().toString());
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