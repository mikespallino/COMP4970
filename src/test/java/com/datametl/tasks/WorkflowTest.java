package com.datametl.tasks;

import com.datametl.jobcontrol.Job;
import com.datametl.jobcontrol.JobManager;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
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
                "\t\"source\": {\n" +
                "\t\t\"host_ip\": \"\",\n" +
                "\t\t\"host_port\": 1234,\n" +
                "\t\t\"path\": \"MOCK_DATA.csv\",\n" +
                "\t\t\"file_type\": \"csv\"\n" +
                "\t},\n" +
                "\t\"rules\": {\n" +
                "\t\t\"transformations\": {\n" +
                "\t\t},\n" +
                "\t\t\"mappings\": {\n" +
                "\t\t\t\"first_name\": \"tester1\",\n" +
                "\t\t\t\"last_name\": \"tester2\",\n" +
                "\t\t\t\"email\": \"tester3\",\n" +
                "\t\t},\n" +
                "\t\t\"filters\": {\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"destination\": {\n" +
                "\t\t\"host_ip\": \"127.0.0.1\",\n" +
                "\t\t\"host_port\": 8983,\n" +
                "\t\t\"username\": \"mspallino\",\n" +
                "\t\t\"password\": \"\",\n" +
                "\t\t\"storage_type\": \"solr\",\n" +
                "\t\t\"destination_location\": \"testcore\"\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"source_header\": \"\",\n" +
                "\t\t\"destination_header\": [\"tester1\", \"tester2\", \"tester3\", \"desty4\", \"new\", \"value\"],\n" +
                "\t\t\"contents\": [],\n" +
                "\t}\n" +
                "}";
        JSONObject etlPacket = new JSONObject(emptyPacketData);
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob("", etlPacket);

        manager.startJob(jobId);
        manager.stopJob(jobId);

        Thread.sleep(2000);
        assertEquals(manager.getJobState(jobId), JobState.SUCCESS);
    }

}