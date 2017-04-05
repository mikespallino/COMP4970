package com.datametl.jobcontrol;

import com.datametl.logging.Logger;
import org.json.JSONObject;

import java.util.List;
import java.util.Vector;

/**
 * Created by mspallino on 1/23/17.
 */
public class Job implements JobInterface, Runnable {

    private Vector<SubJob> subJobs;
    private int retries;
    private JobState state;
    private Thread curThread;
    private int curSubJob;
    private JSONObject packet;
    private Logger log;

    public Job(Vector<SubJob> subJobs, int retries, Logger log) {
        this.retries = retries;
        this.subJobs = subJobs;
        this.state = JobState.NOT_STARTED;
        this.curThread = new Thread(this);
        curSubJob = 0;
        this.log = log;
        for (SubJob sub: subJobs) {
            sub.setParent(this);
        }
    }

    public void run() {
        try {
            int subJobsSize = subJobs.size();
            while (curSubJob < subJobsSize) {
                SubJob sub = subJobs.get(curSubJob);
                sub.start();
                curSubJob++;
                subJobsSize = subJobs.size();
            }

            boolean shouldBreak = false;
            curSubJob = 0;
            int currentRetryCount = 0;
            while (true) {
                subJobsSize = subJobs.size();
                if (curSubJob >= subJobsSize) {
                    curSubJob = 0;
                }
                if (subJobsSize == 0) {
                    break;
                }
                SubJob sub = subJobs.get(curSubJob);
                JobState result = sub.getTaskReturnCode();

                shouldBreak = false;
                switch (result) {
                    case KILLED:
                    case FAILED: {
                        while (currentRetryCount < retries) {
                            log.info("Retrying...");
                            JobState returnState = sub.getTaskReturnCode();

                            if (returnState == JobState.FAILED || returnState == JobState.KILLED) {
                                sub.restart();
                            } else {
                                break;
                            }
                            currentRetryCount++;
                        }
                        state = JobState.FAILED;
                        shouldBreak = true;
                        currentRetryCount = 0;
                    }
                    break;
                    case SUCCESS:
                        subJobs.remove(curSubJob);
                        break;
                    case NOT_STARTED:
                    case RUNNING:
                    default:
                        break;
                }

                if (shouldBreak) {
                    break;
                }

                curSubJob++;
                subJobsSize = subJobs.size();
                if (subJobsSize == 0) {
                    break;
                }
                if (curSubJob == subJobsSize) {
                    curSubJob = 0;
                }
            }
            if (shouldBreak) {
                state = JobState.FAILED;
            } else {
                state = JobState.SUCCESS;
            }
        } catch(Exception ex) {
            ex.printStackTrace();
            state = JobState.FAILED;
        }
        log.info("Finishing job with status: " + state);
    }

    public boolean start() {
        state = JobState.RUNNING;
        curThread.start();
        return true;
    }

    public boolean stop() {
        try {
            curThread.join();
            state = JobState.SUCCESS;
        } catch (InterruptedException ex) {
            state = JobState.FAILED;
            return false;
        }
        return true;
    }

    public boolean restart() {
        stop();
        start();
        return true;
    }

    public boolean isRunning() {
        return state == JobState.RUNNING;
    }

    public boolean kill() {
        for (SubJob sub: subJobs) {
            sub.kill();
        }
        curThread.interrupt();
        state = JobState.KILLED;
        return true;
    }

    public List<SubJob> getSubJobs() {
        return subJobs;
    }

    public boolean addSubJob(SubJob sub) {
        sub.setParent(this);
        subJobs.add(sub);
        return sub.start();
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public JSONObject getETLPacket() {
        return packet;
    }

    public void setETLPacket(JSONObject newPacket) {
        packet = newPacket;
    }

    public String getLogs() {
        return log.getLogs();
    }
}
