package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONObject;

import java.io.File;

/**
 * Created by mspallino on 2/16/17.
 */
public class DataSegmentationTask implements Task {

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent;
    private int documentsPerChunk;
    private long currentBytePosition;
    private long maxFilePosition;

    public DataSegmentationTask(int documentsPerChunk) {
        this.documentsPerChunk = documentsPerChunk;
        this.currentBytePosition = 0;
        this.maxFilePosition = 0;
    }

    public void apply() {
        returnCode = JobState.RUNNING;
        JSONObject etlPacket = parent.getETLPacket();
        Object path = etlPacket.getJSONObject("source").get("path");
        String filePath;
        if (path == null || path == JSONObject.NULL) {
            returnCode = JobState.FAILED;
            throw new RuntimeException("Could not find file!");
        } else {
            filePath = (String) path;
        }
        File fin = new File(filePath);
        if (fin.exists() == false) {
            returnCode = JobState.FAILED;
            throw new RuntimeException("Could not find file!");
        }
        maxFilePosition = fin.length();
        System.out.println(maxFilePosition);
        etlPacket.put("documents_to_read", documentsPerChunk);
        etlPacket.put("current_byte_position", currentBytePosition);
        
        Task extractTask = new ExtractTask();
        SubJob newExtractJob = new SubJob(extractTask);
        newExtractJob.setETLPacket(etlPacket);
        boolean status = parent.getParent().addSubJob(newExtractJob);
        if (status) {
            System.out.println("Added initial ExtractSubJob");
        } else {
            returnCode = JobState.FAILED;
            throw new RuntimeException("Could not insert job in list!");
        }

        /*
         * We want to keep checking to see if the current byte position from the packet has changed.
         * If it has, that means that the last Extract Job we created is done and we are ready to make a new one.
         * If it has not, we should keep sleeping until it has.
         */
        long packetBytePosition;
        while(currentBytePosition < maxFilePosition) {
            packetBytePosition = etlPacket.getLong("current_byte_position");
            if(currentBytePosition == packetBytePosition) {
                try {
                    System.out.println("waiting...");
                    Thread.sleep(3000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    returnCode = JobState.FAILED;
                }
                continue;
            }
            currentBytePosition = packetBytePosition;
            if (currentBytePosition == maxFilePosition) {
                break;
            }
            System.out.println("Previous chunk done! Issuing new ExtractSubJob! bytes:" + currentBytePosition);
            currentBytePosition = packetBytePosition;
            ExtractTask nextChunkExtractTask = new ExtractTask();
            SubJob nextChunkExtractJob = new SubJob(nextChunkExtractTask);
            nextChunkExtractJob.setETLPacket(etlPacket);
            parent.getParent().addSubJob(nextChunkExtractJob);
        }

        System.out.println("DataSegmentationTask - ETLPacket:\n"+etlPacket+"\n");

        returnCode = JobState.SUCCESS;
    }

    public JobState getResult() {
        return returnCode;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }
}
