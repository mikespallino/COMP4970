package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;

/**
 * Created by mspallino on 1/18/17.
 */
public interface Task {
    void apply();
    JobState getResult();
    void setParent(SubJob parent);
    SubJob getParent();
    void setLogger(Logger log);
}
