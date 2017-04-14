package com.datametl.webapp;

import com.datametl.jobcontrol.JobManager;
import com.datametl.logging.LogLevel;
import com.datametl.logging.Logger;

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

    /**
     * Creates a JobManager to be used for the duration of the application.
     * This is called on initialization.
     *
     * @throws ServletException
     */
    @Override
    public void init() throws ServletException {
        super.init();
        Logger.setLogLevel(LogLevel.DEBUG);
        manager = new JobManager();
    }

    public void destroy() {
        super.destroy();
        manager.kill();
    }

}
