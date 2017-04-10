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
    private JobManager manager;

    /**
     * Scheduler Constructor
     * <p>
     * Set the JobManager with the parameter and initializes current thread
     * with the name Scheduler.
     *
     * @param manager
     */
    public Scheduler(JobManager manager) {
        this.manager = manager;
        curThread = new Thread(this, "Scheduler");
    }

    /**
     * Writes workflow to a file
     * <p>
     * Opens the workflow directory and creates a json containing UUID
     * of the workflow by writing ETLPacket information into it.
     *
     * @param jobId
     * @param etlPacket
     */
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

    /**
     * Read workflow from a file
     * <p>
     * Opens the workflow by reading it through a StringBuffer
     * and returning it as a JSONObject
     *
     * @param jobId The UUID of the workflow
     * @return JSONObject containing information of workflow
     */
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

    /**
     * Reloads saved workflows
     * <p>
     * In the case of when DataMETL is restarted, it will
     * reload all the saved workflows from disk and determines
     * the jobState.
     */
    public void readSavedWorkflows() {
        try {
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
                            String stringState = packet.getString("state");
                            JobState state = null;
                            if (stringState.equals("NOT_STARTED")) {
                                state = JobState.NOT_STARTED;
                            } else if (stringState.equals("RUNNING")) {
                                state = JobState.RUNNING;
                            } else if (stringState.equals("SUCCESS")) {
                                state = JobState.SUCCESS;
                            } else if (stringState.equals("FAILED")) {
                                state = JobState.FAILED;
                            } else if (stringState.equals("KILLED")) {
                                state = JobState.KILLED;
                            }
                            manager.addJobWithStatus(jobId, packet, state);
                            isr.close();
                            br.close();
                        }
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    /**
     * Determines whether or not a workflow should start
     * <p>
     * Reads the workflows saved on disk and reads the time to
     * determine if it should start
     */
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
                                isr.close();
                                br.close();
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

    /**
     * Starts a thread
     */
    public void start() {
        curThread.start();
    }

    /**
     *
     */
    public void run() {
        poll();
    }

    /**
     * Kills current thread
     */
    public void kill() {
        curThread.interrupt();
    }

}
