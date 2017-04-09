package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
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
    private Logger log;

    public DataSegmentationTask(int documentsPerChunk, Logger log) {
        this.documentsPerChunk = documentsPerChunk;
        this.currentBytePosition = 0;
        this.maxFilePosition = 0;
        this.log = log;
    }

    public void apply() {
        try {
            returnCode = JobState.RUNNING;
            JSONObject etlPacket = parent.getETLPacket();
            Object path = etlPacket.getJSONObject("source").get("path");
            String filePath;
            if (path == null || path == JSONObject.NULL) {
                returnCode = JobState.FAILED;
                log.error("Could not find file!" + path);
                throw new RuntimeException("Could not find file!");
            } else {
                filePath = (String) path;
            }
            File fin = new File(filePath);
            if (fin.exists() == false) {
                returnCode = JobState.FAILED;
                log.error("Could not find file!" + path);
                throw new RuntimeException("Could not find file!");
            }
            maxFilePosition = fin.length();
            log.info("File bytes: " + maxFilePosition);
            etlPacket.put("documents_to_read", documentsPerChunk);
            etlPacket.put("max_byte_position", maxFilePosition);
            etlPacket.put("current_byte_position", currentBytePosition);

            Task extractTask = new ExtractTask(log);
            SubJob newExtractJob = new SubJob(extractTask);
            newExtractJob.setETLPacket(etlPacket);
            boolean status = parent.getParent().addSubJob(newExtractJob);
            if (status) {
                log.info("Added initial ExtractSubJob");
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
            while (currentBytePosition < maxFilePosition) {
                packetBytePosition = etlPacket.getLong("current_byte_position");
                if (currentBytePosition == packetBytePosition) {
                    try {
                        log.info("waiting...");
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
                log.info("Previous chunk done! Issuing new ExtractSubJob! bytes:" + currentBytePosition);
                currentBytePosition = packetBytePosition;
                ExtractTask nextChunkExtractTask = new ExtractTask(log);
                SubJob nextChunkExtractJob = new SubJob(nextChunkExtractTask);
                nextChunkExtractJob.setETLPacket(etlPacket);
                parent.getParent().addSubJob(nextChunkExtractJob);
            }

            log.debug("DataSegmentationTask - ETLPacket:\n" + etlPacket + "\n");
        } catch(Exception ex) {
            ex.printStackTrace();
            returnCode = JobState.KILLED;
        }

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

    @Override
    public void setLogger(Logger log) {
        this.log = log;
    }
}
