package com.datametl.tasks;

import com.datametl.jobcontrol.Job;
import com.datametl.jobcontrol.JobManager;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.LogLevel;
import com.datametl.logging.Logger;
import org.json.JSONObject;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 2/24/17.
 */
public class WorkflowTest {

    @Test
    public void run() throws Exception {
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
                "    \"storage_type\": \"mock\",\n" +
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
        JSONObject etlPacket = new JSONObject(emptyPacketData);
        JobManager manager = new JobManager();
        Logger.setLogLevel(LogLevel.DEBUG);
        UUID jobId = manager.addJob("", etlPacket);

        ExportDecisionFactory.addNewExporter("mock", MockExporterTask.class);

        manager.startJob(jobId);
        manager.stopJob(jobId);

        Thread.sleep(2000);

        assertNotEquals(manager.getLogs(jobId), "");
        assertEquals(manager.getJobState(jobId), JobState.SUCCESS);
    }

}