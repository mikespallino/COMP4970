package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.json.JSONObject;

/**
 * Created by mspallino on 4/9/17.
 */
public class MockExporterTask implements Task, ExportInterface {

    private JobState state = JobState.NOT_STARTED;
    private SubJob parent;

    public MockExporterTask() {
        state = JobState.RUNNING;

    }

    @Override
    public void initiateConnection() {

    }

    @Override
    public void terminateConnection() {

    }

    @Override
    public void retrieveContents(JSONObject packet) {

    }

    @Override
    public void exportToDSS() {

    }

    @Override
    public void apply() {
        state = JobState.SUCCESS;
    }

    @Override
    public JobState getResult() {
        return state;
    }

    @Override
    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    @Override
    public SubJob getParent() {
        return parent;
    }

    @Override
    public void setLogger(Logger log) {

    }
}
