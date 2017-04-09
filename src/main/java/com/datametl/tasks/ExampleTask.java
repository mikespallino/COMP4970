package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.jobcontrol.Job;
import com.datametl.logging.Logger;

/**
 * Created by mspallino on 1/18/17.
 */
public class ExampleTask implements Task {

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;
    private Logger log;

    /**
     * This is just meant to be an example of how this "should" work.
     */
    public void apply() {
        returnCode = JobState.RUNNING;
        try {
            Thread.sleep(4000);
            log.info("Did the thing!");

            if (parent != null) {
                log.info("Got my parent's ETL packet!: " + parent.getETLPacket());
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

    @Override
    public void setLogger(Logger log) {
        this.log = log;
    }
}
