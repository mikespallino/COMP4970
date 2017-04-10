package com.datametl.jobcontrol;

import java.util.*;

import com.datametl.logging.Logger;
import com.datametl.tasks.DataSegmentationTask;
import com.datametl.tasks.Task;
import org.json.*;


/**
 * Created by mspallino on 1/30/17.
 */
public class JobManager implements Runnable {

    private Map<UUID, Job> jobs;
    private Map<String, UUID> namedJobs;
    private Thread curThread;
    private Scheduler scheduler;
    private Logger log;

    /**
     * JobManager Constructor
     *
     */
    public JobManager() {
        jobs = new HashMap<UUID, Job>();
        namedJobs = new HashMap<String, UUID>();
        scheduler = new Scheduler(this);
        scheduler.readSavedWorkflows();
        scheduler.start();
        log = new Logger("JobManager");
        curThread = new Thread(this, "JobManager");
        curThread.start();
    }

    //Path & file type has been altered

    /**
     * Adds a Job
     *
     * @param name name of Job
     * @param uiData JSONObject
     * @return new UUID
     */
    public UUID addJob(String name, JSONObject uiData) {
        UUID newId = UUID.randomUUID();
        Logger newLogger = getNewLogger(newId);

        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250, newLogger);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3, newLogger);

        if (name.equals("")) {
            name = newId.toString();
        }
        uiData.put("name", name);
        job.setETLPacket(uiData);

        for(String n : namedJobs.keySet()) {
            if (name.equals(n)) {
                return null;
            }
        }
        namedJobs.put(name, newId);
        jobs.put(newId, job);
        scheduler.saveWorkflow(newId, uiData);
        return newId;
    }

    /**
     * Restarts a job based on name
     *
     * @param name name of Job
     * @param uiData JSONObject
     * @return UUID of Job
     */
    public UUID resubmitJob(String name, JSONObject uiData) {
        System.out.println(name);
        UUID jobId = namedJobs.get(name);
        namedJobs.remove(name);
        jobs.remove(jobId);

        Logger newLogger = getNewLogger(jobId);

        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250, newLogger);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3, newLogger);

        if (name.equals("")) {
            name = jobId.toString();
        }
        uiData.put("name", name);
        job.setETLPacket(uiData);

        for(String n : namedJobs.keySet()) {
            if (name.equals(n)) {
                return null;
            }
        }
        namedJobs.put(name, jobId);
        jobs.put(jobId, job);
        scheduler.saveWorkflow(jobId, uiData);
        return jobId;
    }

    /**
     * Adds a Job to the list with state
     *
     * @param jobId UUID of job
     * @param packet JSONObject
     * @param state JobState
     */
    public void addJobWithStatus(UUID jobId, JSONObject packet, JobState state) {
        Vector<SubJob> subJobs = new Vector<SubJob>();
        Job job = new Job(subJobs, 3, getNewLogger(jobId));
        job.setState(state);
        job.setETLPacket(packet);
        String name = packet.getString("name");

        namedJobs.put(name, jobId);
        jobs.put(jobId, job);
    }

    /**
     * Removes a Job
     *
     * @param oldJobId UUID of Job
     */
    public void removeJob(UUID oldJobId) {
        jobs.remove(oldJobId);
    }

    /**
     * Starts a job
     *
     * @param jobId UUID of Job
     * @return true or false
     */
    public boolean startJob(UUID jobId) {
        return jobs.get(jobId).start();
    }

    /**
     * Kills a Job
     *
     * @param jobId UUID of Job
     * @return true or false
     */
    public boolean killJob(UUID jobId) {
        return jobs.get(jobId).kill();
    }

    /**
     * Stops a Job
     *
     * @param jobId UUID of Job
     * @return true or false
     */
    public boolean stopJob(UUID jobId) {
        return jobs.get(jobId).stop();
    }

    /**
     * Return the jobID from ETLPacket in JSONObject
     *
     * @param jobId UUID
     * @return jobID in JSONObject
     */
    public JSONObject getJobETLPacket(UUID jobId) {
        return jobs.get(jobId).getETLPacket();
    }

    /**
     * Starts the process of JobManager
     * <p>
     * Goes through a list of Jobs and logs the status of each Job
     *
     */
    public void run() {
        int jobIndex = 0;
        int jobSize = jobs.size();
        List<UUID> jobIds = new ArrayList<UUID>();
        jobIds.addAll(jobs.keySet());
        while (true) {
            if (jobSize == 0 || jobIndex > jobSize) {
                log.debug("No jobs...");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    log.error("Uh-oh");
                }
                jobSize = jobs.size();
                jobIds.clear();
                jobIds.addAll(jobs.keySet());
                continue;
            }
            UUID curJobId = jobIds.get(jobIndex);
            Job curJob = jobs.get(curJobId);
            JobState curState = curJob.getState();
            //TODO: update UI with job state info
            switch (curState) {
                case FAILED:
                    log.info("Job [" + curJobId + "] STATE: " + curState + " [RED]");
                    break;
                case KILLED:
                    log.info("Job [" + curJobId + "] STATE: " + curState + "  [BLACK]");
                    break;
                case NOT_STARTED:
                    log.info("Job [" + curJobId + "] STATE: " + curState + "  [GRAY]");
                    break;
                case RUNNING:
                    log.info("Job [" + curJobId + "] STATE: " + curState + "  [YELLOW]");
                    break;
                case SUCCESS:
                    log.info("Job [" + curJobId + "] STATE: " + curState + "  [GREEN]");
                    break;
                default:
                    break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                log.error("Uh-oh");
            }
            jobIndex++;
            jobSize = jobs.size();
            jobIds.clear();
            jobIds.addAll(jobs.keySet());
            if (jobIndex == jobSize) {
                jobIndex = 0;
            }
        }
    }

    /**
     * Terminates JobManager after saving jobs to disk
     *
     * @return true
     */
    public boolean kill() {
        for(UUID id: jobs.keySet()) {
            Job j = jobs.get(id);
            JSONObject packet = j.getETLPacket();
            packet.put("state", jobs.get(id).getState());
            scheduler.saveWorkflow(id, packet);
            j.kill();
        }
        killScheduler();
        log.getLogs();
        curThread.interrupt();
        return true;
    }

    /**
     * Return Map of UUID and Jobs
     *
     * @return map of UUID and Jobs
     */
    public Map<UUID, Job> getJobs() {
        return jobs;
    }

    /**
     * Returns the JobState of a Job
     *
     * @param id UUID of a Job
     * @return JobState
     */
    public JobState getJobState(UUID id) {
        return jobs.get(id).getState();
    }

    /**
     * Returns a Job by name
     *
     * @param name name of a Job
     * @return Job
     */
    public Job getJobByName(String name) {
        return jobs.get(namedJobs.get(name));
    }

    /**
     * Returns a map of name and Jobs
     *
     * @return namedJobs
     */
    public Map<String, UUID> getNamedJobs() {
        return namedJobs;
    }

    /**
     * Restart specific Workflow with new configurations
     *
     * @param jobid UUID of a Job
     * @param packet JSONObject of new user configurations
     */
    public void resubmit(UUID jobid, JSONObject packet) {
        removeJob(jobid);
        Logger newLogger = new Logger(jobid.toString());

        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250, newLogger);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3, newLogger);
        job.setETLPacket(packet);

        jobs.put(jobid, job);
    }

    /**
     * Kills the Scheduler
     *
     */
    public void killScheduler() {
        scheduler.kill();
    }

    private Logger getNewLogger(UUID jobId) {
        Logger newLogger = new Logger(jobId.toString());
        return newLogger;
    }

    /**
     * Get logs of Job
     *
     * @param jobId UUID of Job
     * @return string containing the logs
     */
    public String getLogs(UUID jobId) {
        Job j = jobs.get(jobId);
        if (j!= null) {
            return j.getLogs();
        } else {
            return null;
        }
    }
}
