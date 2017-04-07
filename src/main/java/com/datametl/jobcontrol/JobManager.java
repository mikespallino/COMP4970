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

    public void addJobWithStatus(UUID jobId, JSONObject packet, JobState state) {
        Vector<SubJob> subJobs = new Vector<SubJob>();
        Job job = new Job(subJobs, 3, getNewLogger(jobId));
        job.setState(state);
        job.setETLPacket(packet);
        String name = packet.getString("name");

        namedJobs.put(name, jobId);
        jobs.put(jobId, job);
    }

    public void removeJob(UUID oldJobId) {
        jobs.remove(oldJobId);
    }

    public boolean startJob(UUID jobId) {
        return jobs.get(jobId).start();
    }

    public boolean killJob(UUID jobId) {
        return jobs.get(jobId).kill();
    }

    public boolean stopJob(UUID jobId) {
        return jobs.get(jobId).stop();
    }

    public JSONObject getJobETLPacket(UUID jobId) {
        return jobs.get(jobId).getETLPacket();
    }

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

    public Map<UUID, Job> getJobs() {
        return jobs;
    }

    public JobState getJobState(UUID id) {
        return jobs.get(id).getState();
    }

    public Job getJobByName(String name) {
        return jobs.get(namedJobs.get(name));
    }

    public Map<String, UUID> getNamedJobs() {
        return namedJobs;
    }

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

    public void killScheduler() {
        scheduler.kill();
    }

    private Logger getNewLogger(UUID jobId) {
        Logger newLogger = new Logger(jobId.toString());
        return newLogger;
    }

    public String getLogs(UUID jobId) {
        Job j = jobs.get(jobId);
        if (j!= null) {
            return j.getLogs();
        } else {
            return null;
        }
    }
}
