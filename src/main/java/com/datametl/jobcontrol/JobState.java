package com.datametl.jobcontrol;

/**
 * Created by mspallino on 1/23/17.
 */

/**
 * NOT_STARTED is a state of a job that has not started.
 * RUNNING is a state of a job that is running.
 * SUCCESS is a state of a job that has finished successfully.
 * FAILED is a state of a job that has finished unsuccessfully.
 * KILLED is a state of a job that has been terminated.
 */
public enum JobState {
    NOT_STARTED, RUNNING, SUCCESS, FAILED, KILLED
}
