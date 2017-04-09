package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 1/16/17.
 */
public class SubJobTest {
    private ExampleTask exampleTask;

    @Before
    public void setUp() throws Exception {
        this.exampleTask = new ExampleTask();
    }

    @Test
    public void start() throws Exception {
        SubJob test = new SubJob(this.exampleTask);
        boolean result = test.start();
        assertTrue(result);
        assertTrue(test.isRunning());
        test.kill();
    }

    @Test
    public void stop() throws Exception {
        SubJob test = new SubJob(this.exampleTask);
        boolean result = test.stop();
        assertTrue(result);
        assertFalse(test.isRunning());
        test.kill();
    }

    @Test
    public void restart() throws Exception {
        SubJob test = new SubJob(this.exampleTask);
        boolean result = test.restart();
        assertTrue(result);
        assertTrue(test.isRunning());
        test.kill();
    }

    @Test
    public void isRunning() throws Exception {
        SubJob test = new SubJob(this.exampleTask);
        assertFalse(test.isRunning());
        test.start();
        assertTrue(test.isRunning());
        test.stop();
        assertFalse(test.isRunning());
        test.kill();
    }

    @Test
    public void successfulExecution() throws Exception {
        SubJob test = new SubJob(this.exampleTask);
        test.start();
        test.stop();
        assertEquals(JobState.SUCCESS, test.getTaskReturnCode());
    }

    @Test
    public void badExecution() throws Exception {
        //INFO: This test is not that good.
        SubJob test = new SubJob(this.exampleTask);
        test.start();
        assertTrue(test.isRunning());
        System.out.println("Waiting to kill...");
        Thread.sleep(2000);
        test.kill();
        Thread.sleep(5000);
        assertEquals(JobState.KILLED, test.getTaskReturnCode());
    }

}