package com.datametl.webapp;

import com.datametl.jobcontrol.Job;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

/**
 *
 */
@WebServlet("/getlogs")
public class GetLogs extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        String requestedJob = request.getParameter("jobid");
        UUID jobId;
        try {
            jobId = UUID.fromString(requestedJob);
        } catch (java.lang.IllegalArgumentException ex) {
            jobId = Index.manager.getNamedJobs().get(requestedJob);
        }
        String logs = Index.manager.getLogs(jobId);
        PrintWriter out = response.getWriter();
        out.println(logs);
        return;
    }
}
