package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 2/2/17.
 */
public class JobManagerTest {

    private Vector<SubJob> subJobs;
    private Job job;
    private JSONObject etlPacket;

    @Before
    public void setUp() throws Exception {
        subJobs = new Vector<SubJob>();
        for(int i = 0; i < 3; ++i) {
            subJobs.add(new SubJob(new ExampleTask()));
        }

        this.job = new Job(this.subJobs, 3);

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
        this.etlPacket = new JSONObject(emptyPacketData);
    }

    @Test
    public void addJob() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job, this.etlPacket);
        boolean started = manager.startJob(jobId);
        assertTrue(started);

        setUp();
        UUID newJobId = manager.addJob(job, this.etlPacket);
        boolean newJobStarted = manager.startJob(newJobId);
        assertTrue(newJobStarted);

        Thread.sleep(1000);

        manager.stopJob(jobId);
        manager.stopJob(newJobId);
        Thread.sleep(2000);
    }

    @Test
    public void removeJob() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job, this.etlPacket);
        boolean started = manager.startJob(jobId);
        assertTrue(started);
        manager.removeJob(jobId);
    }

    @Test
    public void  getETLPacket() throws Exception {
        //TODO: we can do better
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job, this.etlPacket);

        JSONObject packet = job.getETLPacket();
        System.out.println(packet);

        assertNotNull(packet.getJSONObject("data"));
        assertNotNull(packet.get("source"));
        assertNotNull(packet.get("rules"));
        assertNotNull(packet.get("destination"));

        JSONObject managerPacket = manager.getJobETLPacket(jobId);
        System.out.println(managerPacket);

        assertNotNull(managerPacket.getJSONObject("data"));
        assertNotNull(managerPacket.get("source"));
        assertNotNull(managerPacket.get("rules"));
        assertNotNull(managerPacket.get("destination"));
    }

}