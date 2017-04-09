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

    public SubJob(Task t) {
        this.t = t;
        curThread = new Thread(this);
        this.t.setParent(this);
        parent = null;
    }

    public boolean start() {
        curThread.start();
        return true;
    }

    public boolean stop() {
        try {
            curThread.join();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean restart() {
        stop();
        start();
        return true;
    }

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

    public JobState getTaskReturnCode() {
        return t.getResult();
    }

    public boolean isRunning() {
        return curThread.isAlive();
    }

    public void run() {
        t.apply();
    }

    public Job getParent() {
        return parent;
    }

    public void setParent(Job parent) {
        this.parent = parent;
    }

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

    public void setETLPacket(JSONObject etlPacket) {
        this.etlPacket = etlPacket;
    }
}
