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
    public UUID addJob(Job newJob) {
        UUID newId = UUID.randomUUID();
        String emptyPacketData = "{\n" +
                "\t\"source\": {\n" +
                "\t\t\"host_ip\": \"\",\n" +
                "\t\t\"host_port\": 1234,\n" +
                "\t\t\"path\": \"MOCK_DATA.xml\",\n" +
                "\t\t\"file_type\": \"xml\"\n" +
                "\t},\n" +
                "\t\"rules\": {\n" +
                "\t\t\"transformations\": {\n" +
                "\t\t\t\"transform1\": {\n" +
                "\t\t\t\t\"source_column\": \"id\",\n" +
                "\t\t\t\t\"new_field\": \"tester3\",\n" +
                "\t\t\t\t\"transform\": \"ADD 3\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"transform2\": {\n" +
                "\t\t\t\t\"source_column\": \"id\",\n" +
                "\t\t\t\t\"new_field\": null,\n" +
                "\t\t\t\t\"transform\": \"MULT 2\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"transform3\": {\n" +
                "\t\t\t\t\"source_column\": \"id\",\n" +
                "\t\t\t\t\"new_field\": \"desty4\",\n" +
                "\t\t\t\t\"transform\": \"MULT 4\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"mappings\": {\n" +
                "\t\t\t\"tester1\": \"desty2\",\n" +
                "\t\t\t\"tester2\": \"desty1\"\n" +
                "\t\t},\n" +
                "\t\t\"filters\": {\n" +
                "\t\t\t\"filter1\": {\n" +
                "\t\t\t\t\"source_column\": \"tester4\",\n" +
                "\t\t\t\t\"filter_value\": \"tester\",\n" +
                "\t\t\t\t\"equality_test\": \"eq\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"destination\": {\n" +
                "\t\t\"host_ip\": \"\",\n" +
                "\t\t\"host_port\": 1234,\n" +
                "\t\t\"username\": \"\",\n" +
                "\t\t\"password\": \"\",\n" +
                "\t\t\"storage_type\": \"\"\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"source_header\": \"\",\n" +
                "\t\t\"destination_header\": [\"tester1\", \"tester2\", \"tester3\", \"desty4\"],\n" +
                "\t\t\"contents\": [],\n" +
                "\t}\n" +
                "}";
        JSONObject etlPacket = new JSONObject(emptyPacketData);
        newJob.setETLPacket(etlPacket);
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
}
