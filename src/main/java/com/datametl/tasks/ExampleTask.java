package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.jobcontrol.Job;

/**
 * Created by mspallino on 1/18/17.
 */
public class ExampleTask implements Task {

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;

    /**
     * This is just meant to be an example of how this "should" work.
     */
    public void apply() {
        returnCode = JobState.RUNNING;
        try {
            Thread.sleep(4000);
            System.out.println("Did the thing!");

            if (parent != null) {
                System.out.println("Got my parent's ETL packet!: " + parent.getETLPacket());
            }
        } catch (Exception ex) {
            returnCode = JobState.KILLED;
            ex.printStackTrace();
            return;
        }
        returnCode = JobState.SUCCESS;
    }

    public JobState getResult() {
        return returnCode;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }
}
