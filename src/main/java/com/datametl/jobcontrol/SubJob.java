package com.datametl.jobcontrol;

import com.datametl.tasks.Task;
import org.json.JSONObject;

/**
 * Created by mspallino on 1/16/17.
 */
public class SubJob implements SubJobInterface, Runnable {

    private Task t;
    private Thread curThread;
    private Job parent;
    private JSONObject etlPacket;

    /**
     * SubJob Constructor
     *
     * @param t Extract,Rules, or Export
     * @see Task
     */
    public SubJob(Task t) {
        this.t = t;
        curThread = new Thread(this);
        this.t.setParent(this);
        parent = null;
    }

    /**
     * Start the thread
     *
     * @return true
     */
    public boolean start() {
        curThread.start();
        return true;
    }

    /**
     * Stops the SubJob
     * <p>
     * Attempts to finish the current task of the thread. If success,
     * returns true for stopping. Otherwise, false.
     *
     * @return Success state of stopping the thread
     */
    public boolean stop() {
        try {
            curThread.join();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Restarts the SubJob
     *
     * @return true
     */
    public boolean restart() {
        stop();
        start();
        return true;
    }

    /**
     * Terminates the thread
     *
     * @return true
     */
    public boolean kill() {
        curThread.interrupt();
        try {
            Thread.sleep(200);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        curThread.stop();
        return true;
    }

    /**
     * Returns the JobState of SubJob
     *
     * @return JobState
     * @see JobState
     */
    public JobState getTaskReturnCode() {
        return t.getResult();
    }

    /**
     * Checks if the current thread is alive
     *
     * @return true if thread is alive
     */
    public boolean isRunning() {
        return curThread.isAlive();
    }

    /**
     * Runs the SubJob's apply method
     *
     */
    public void run() {
        t.apply();
    }

    /**
     * Returns the parent of SubJob
     *
     * @return parent
     */
    public Job getParent() {
        return parent;
    }

    /**
     * Sets parent of SubJob
     *
     * @param parent Job
     */
    public void setParent(Job parent) {
        this.parent = parent;
    }

    /**
     * Returns ETLPacket
     *
     * @return ETLPacket
     */
    public JSONObject getETLPacket() {
        if (etlPacket == null) {
            if (parent != null) {
                etlPacket = parent.getETLPacket();
            } else {
                return null;
            }
        }
        return etlPacket;
    }

    /**
     * Sets the ETLPacket
     *
     * @param etlPacket JSONOBject
     */
    public void setETLPacket(JSONObject etlPacket) {
        this.etlPacket = etlPacket;
    }
}
