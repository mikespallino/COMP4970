package com.datametl.jobcontrol;

import java.util.*;

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

    public JobManager() {
        jobs = new HashMap<UUID, Job>();
        namedJobs = new HashMap<String, UUID>();
        scheduler = new Scheduler();
        curThread = new Thread(this, "JobManager");
        curThread.start();
    }

    //Path & file type has been altered
    public UUID addJob(String name, JSONObject uiData) {
        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3);

        UUID newId = UUID.randomUUID();
        job.setETLPacket(uiData);
        if (name.equals("")) {
            name = newId.toString();
        }

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
                System.out.println("No jobs...");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    System.out.println("Uh-oh");
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
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + " [RED]");
                    break;
                case KILLED:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [BLACK]");
                    break;
                case NOT_STARTED:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [GRAY]");
                    break;
                case RUNNING:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [YELLOW]");
                    break;
                case SUCCESS:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [GREEN]");
                    break;
                default:
                    break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                System.out.println("Uh-oh");
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
            jobs.get(id).kill();
        }
        killScheduler();
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

        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3);
        job.setETLPacket(packet);

        jobs.put(jobid, job);
    }

    public void killScheduler() {
        scheduler.kill();
    }
}
