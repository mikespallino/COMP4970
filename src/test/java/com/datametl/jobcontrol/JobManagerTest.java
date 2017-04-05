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
    private JSONObject etlPacket;

    @Before
    public void setUp() throws Exception {

        String emptyPacketData = "{\n" +
                "  \"schedule\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"destination_header\": [\n" +
                "      \"tester1\",\n" +
                "      \"tester2\",\n" +
                "      \"tester3\",\n" +
                "      \"desty4\",\n" +
                "      \"new\",\n" +
                "      \"value\"\n" +
                "    ],\n" +
                "    \"contents\": [],\n" +
                "    \"source_header\": \"\"\n" +
                "  },\n" +
                "  \"destination\": {\n" +
                "    \"storage_type\": \"mysql\",\n" +
                "    \"host_ip\": \"127.0.0.1\",\n" +
                "    \"password\": \"test\",\n" +
                "    \"host_port\": \"3306\",\n" +
                "    \"username\": \"root\"\n" +
                "  },\n" +
                "  \"name\": \"171c1e33-b330-4677-b271-df44bbc25c66\",\n" +
                "  \"rules\": {\n" +
                "    \"mappings\": {},\n" +
                "    \"transformations\": {},\n" +
                "    \"filters\": {}\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"path\": \"/Users/mspallino/School/Senior Year/Spring 2017/Software Engineering/Project/DataMETL/MOCK_DATA.csv\",\n" +
                "    \"host_ip\": null,\n" +
                "    \"host_port\": null,\n" +
                "    \"file_type\": \"csv\"\n" +
                "  },\n" +
                "  \"time\": \"\",\n" +
                "  \"state\": \"NOT_STARTED\"\n" +
                "}";
        this.etlPacket = new JSONObject(emptyPacketData);
    }

    @Test
    public void addJob() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob("", this.etlPacket);
        boolean started = manager.startJob(jobId);
        assertTrue(started);

        setUp();
        UUID newJobId = manager.addJob("", this.etlPacket);
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
        UUID jobId = manager.addJob("", this.etlPacket);
        boolean started = manager.startJob(jobId);
        assertTrue(started);
        manager.removeJob(jobId);
    }

    @Test
    public void  getETLPacket() throws Exception {
        //TODO: we can do better
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob("", this.etlPacket);

        JSONObject managerPacket = manager.getJobETLPacket(jobId);
        System.out.println(managerPacket);

        assertNotNull(managerPacket.getJSONObject("data"));
        assertNotNull(managerPacket.get("source"));
        assertNotNull(managerPacket.get("rules"));
        assertNotNull(managerPacket.get("destination"));
    }

}