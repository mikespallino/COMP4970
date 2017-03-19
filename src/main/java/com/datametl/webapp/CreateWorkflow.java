package com.datametl.webapp;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

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
            out.println(key + ": \n");
            String[] val = requestParams.get(key);
            for (int i = 0; i < val.length; ++i) {
                out.println(val[i] + ", ");
            }
            out.println("");
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