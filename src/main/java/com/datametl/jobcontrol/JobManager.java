package com.datametl.jobcontrol;

import java.util.*;
import org.json.*;


/**
 * Created by mspallino on 1/30/17.
 */
public class JobManager implements Runnable {

    private Map<UUID, Job> jobs;
    private Thread curThread;

    public JobManager() {
        jobs = new HashMap<UUID, Job>();
        curThread = new Thread(this);
        curThread.start();
    }

    //Path & file type has been altered
    public UUID addJob(Job newJob, JSONObject uiData) {
        UUID newId = UUID.randomUUID();
        newJob.setETLPacket(uiData);
        jobs.put(newId, newJob);
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
        curThread.interrupt();
        return true;
    }

    public Map<UUID, Job> getJobs() {
        return jobs;
    }
}
