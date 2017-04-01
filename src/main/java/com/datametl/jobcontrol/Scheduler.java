package com.datametl.jobcontrol;

import com.datametl.webapp.Index;
import org.json.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by mspallino on 3/29/17.
 */
public class Scheduler implements Runnable {

    private Thread curThread;

    public Scheduler() {
        curThread = new Thread(this, "Scheduler");
        curThread.start();
    }

    public void saveWorkflow(UUID jobId, JSONObject etlPacket) {
        try {
            File dir = new File("workflows");
            File f = new File("workflows/" + jobId.toString() + ".json");
            dir.mkdir();
            f.createNewFile();

            PrintWriter out = new PrintWriter(f);
            out.println(etlPacket.toString());
            out.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }

    }

    public JSONObject readWorkflow(UUID jobId) {
        try {
            File f = new File("workflows/" + jobId.toString() + ".json");
            InputStreamReader isr = new InputStreamReader(new FileInputStream(f));
            BufferedReader br = new BufferedReader(isr);
            StringBuffer buff = new StringBuffer();
            String data;
            while((data = br.readLine()) != null) {
                buff.append(data);
            }

            return new JSONObject(buff.toString());
        } catch(IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public void poll() {
        try {
            while(true) {
                File dir = new File("workflows");
                if (dir.exists()) {
                    File[] directoryContents = dir.listFiles();
                    if (directoryContents != null) {
                        for (File f : directoryContents) {
                            String fileName = f.getName();
                            if (fileName.endsWith(".json")) {
                                UUID jobId = UUID.fromString(fileName.split(".json")[0]);
                                InputStreamReader isr = new InputStreamReader(new FileInputStream(f));
                                BufferedReader br = new BufferedReader(isr);
                                StringBuffer buff = new StringBuffer();
                                String data;
                                while ((data = br.readLine()) != null) {
                                    buff.append(data);
                                }
                                JSONObject packet = new JSONObject(buff.toString());
                                String s = packet.getString("schedule");
                                String t = packet.getString("time");

                                Date date = new Date();
                                String dow = new SimpleDateFormat("EE").format(date);
                                String time = new SimpleDateFormat("HH:mm").format(date);

                                //TODO: Implement some sort of black-list so that we don't re run a job if it finishes in under a minute
                                for (String dayToRun : s.split(",")) {
                                    if (dayToRun.equals(dow)) {
                                        if (time.equals(t)) {
                                            if (Index.manager.getJobState(jobId) != JobState.RUNNING) {
                                                System.out.println("SCHEDULER: resubmitting: " + jobId.toString());
                                                Index.manager.resubmit(jobId, packet);
                                                Index.manager.startJob(jobId);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        //INFO: There are no workflows saved so we should wait a bit until we check again
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                            return;
                        }
                    }
                } else {
                    //INFO: Make the directory if it isn't there yet, then sleep
                    dir.mkdir();
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        return;
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    return;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void run() {
        poll();
    }

    public void kill() {
        curThread.interrupt();
    }

}
