package com.datametl.webapp;

import com.datametl.jobcontrol.JobManager;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by mspallino on 3/20/17.
 */
public class Index extends HttpServlet {
    private static final long serialVersionUID = 1L;

    public static JobManager manager;

    @Override
    public void init() throws ServletException {
        super.init();
        manager = new JobManager();
    }

    public void destroy() {
        super.destroy();
        manager.kill();
    }

}
