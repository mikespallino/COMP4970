package com.datametl.logging;

import com.sun.javafx.binding.StringFormatter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by mspallino on 4/3/17.
 */
public class Logger {

    private StringBuilder builder;
    private String name;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static LogLevel level = LogLevel.ERROR;
    private boolean firstCheck = false;

    public Logger(String name) {
        this.name = name;
        builder = new StringBuilder();
        builder.append("\nSTART LOG: ")
                .append(dateFormat.format(new Date()))
                .append("\n===========================================\n");

        File f = new File("logs/" + this.name + ".log");
        if(f.exists()) {
            try {
                FileReader in = new FileReader(f);
                BufferedReader br = new BufferedReader(in);
                String data;
                while ((data = br.readLine()) != null) {
                    builder.append(data);
                }
                in.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public synchronized void debug(String msg) {
        if (level == LogLevel.DEBUG) {
            builder.append("[")
                    .append(dateFormat.format(new Date()))
                    .append("] - ")
                    .append(LogLevel.DEBUG)
                    .append(" - ")
                    .append(msg)
                    .append("\n");
        }
    }

    public synchronized void info(String msg) {
        if (level == LogLevel.INFO || level == LogLevel.DEBUG) {
            builder.append("[")
                    .append(dateFormat.format(new Date()))
                    .append("] - ")
                    .append(LogLevel.DEBUG)
                    .append(" - ")
                    .append(msg)
                    .append("\n");
        }
    }

    public synchronized void error(String msg) {
        if (level == LogLevel.ERROR || level == LogLevel.INFO || level == LogLevel.DEBUG) {
            builder.append("[")
                    .append(dateFormat.format(new Date()))
                    .append("] - ")
                    .append(LogLevel.DEBUG)
                    .append(" - ")
                    .append(msg)
                    .append("\n");
        }
    }

    public static void setLogLevel(LogLevel newLevel) {
        level = newLevel;
    }

    public String getLogs() {
        if (!firstCheck) {
            builder.append("END LOG")
                    .append("\n===========================================\n");
            firstCheck = true;
        }
        String logs = builder.toString();

        File logDir = new File("logs/");
        if (!logDir.exists()) {
            logDir.mkdir();
        }

        File f = new File("logs/" + this.name + ".log");
        try {
            FileWriter out = new FileWriter(f, true);
            out.append(logs);
            out.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return logs;
    }
}
