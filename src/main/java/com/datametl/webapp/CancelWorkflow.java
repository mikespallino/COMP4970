package com.datametl.webapp;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;

/**
 * Servlet implementation class /cancelworkflow
 */
@WebServlet("/cancelworkflow")
public class CancelWorkflow extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * GET request for killing a Job running in the JobManager.
     * <p>
     * usage: GET localhost:8080/DataMETL/cancelworkflow?jobid=UUID4
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        UUID jobId = UUID.fromString(request.getParameter("jobid"));
        boolean status = Index.manager.killJob(jobId);

        if (status) {
            //INFO: This is good, just return.
        } else {
            response.sendError(400, "Could not stop job");
        }
    }

}