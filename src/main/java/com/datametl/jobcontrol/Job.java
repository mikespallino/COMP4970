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

    /**
     * Job Constructor
     * <p>
     * Initialize a vector of SubJobs, retries, and the log
     *
     * @param subJobs list of SubJobs
     * @param retries the amount of times the SubJob will attempt to run
     * @param log log message
     */
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

    /**
     * Run the list of SubJobs and determines what fails and succeeds
     * <p>
     * Iterates over the list of SubJobs and attempts to run the SubJob
     * until the SubJob succeeds. If success, it will remove from the list.
     * Otherwise, it will run the SubJob up to retries limit and set
     * JobState to FAILED when it doesn't succeed during the retries.
     *
     */
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

    /**
     * Start the SubJob and thread
     *
     * @return true
     */
    public boolean start() {
        state = JobState.RUNNING;
        curThread.start();
        return true;
    }

    /**
     * Attempts to stop SubJob and returns status
     * <p>
     * Sets the state and return to true if the SubJob completes
     * Otherwise, set JobState to failed and return false
     *
     * @return true or false determined by success of stopping thread
     */
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

    /**
     * Restarts a SubJob
     *
     * @return true
     */
    public boolean restart() {
        stop();
        start();
        return true;
    }

    /**
     * Checks if the SubJob is running or not
     *
     * @return true or false
     */
    public boolean isRunning() {
        return state == JobState.RUNNING;
    }

    /**
     * Kills subJob
     *
     * @return success on killing a SubJob
     */
    public boolean kill() {
        for (SubJob sub: subJobs) {
            sub.kill();
        }
        subJobs.clear();
        state = JobState.KILLED;
        curThread.stop();
        return true;
    }

    /**
     * Returns a list of SubJobs
     *
     * @return a list of subJobs
     */
    public List<SubJob> getSubJobs() {
        return subJobs;
    }

    /**
     * Sets the parent of the sub and adds it into the list of subJobs
     * It will return success if started properly
     *
     * @param sub SubJob
     * @return success or failure of starting the sub
     */
    public boolean addSubJob(SubJob sub) {
        sub.setParent(this);
        subJobs.add(sub);
        return sub.start();
    }

    /**
     * Returns a SubJob state
     *
     * @return state of a job
     */
    public JobState getState() {
        return state;
    }

    /**
     * Sets a SubJob state
     *
     * @param state Sets the jobState of a job
     */
    public void setState(JobState state) {
        this.state = state;
    }

    /**
     * Returns ETLPacket
     *
     * @return ETLPacket
     */
    public JSONObject getETLPacket() {
        return packet;
    }

    /**
     * Sets the ETLPacket
     *
     * @param newPacket JSONObject with the same template as ETLPacket
     */
    public void setETLPacket(JSONObject newPacket) {
        packet = newPacket;
    }

    /**
     * Get Logs
     *
     * @return Logs
     */
    public String getLogs() {
        return log.getLogs();
    }
}
