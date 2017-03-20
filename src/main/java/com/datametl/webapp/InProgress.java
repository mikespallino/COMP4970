package com.datametl.webapp;

import com.datametl.jobcontrol.Job;
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
@WebServlet("/inprogress")
public class InProgress extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        Map<UUID, Job> jobs = Index.manager.getJobs();
        JSONObject statusJson = new JSONObject();

        //TODO: Return everything later
        for(UUID id: jobs.keySet()) {
            statusJson.put(id.toString(), jobs.get(id).getState());
        }

        PrintWriter out = response.getWriter();
        out.println(statusJson.toString());
    }


    @Override
    public void init() throws ServletException {
        super.init();
    }

    public void destroy() {
        super.destroy();
    }

}