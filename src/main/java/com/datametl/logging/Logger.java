package com.datametl.logging;

import com.sun.javafx.binding.StringFormatter;

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

    public Logger(String name) {
        this.name = name;
        builder = new StringBuilder();
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
        return builder.toString();
    }
}
