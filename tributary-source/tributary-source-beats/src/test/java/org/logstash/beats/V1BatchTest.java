package org.logstash.beats;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;

/** V1BatchTest. */
public class V1BatchTest {
    public static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new AfterburnerModule());

    private V1Batch batch;

    @Before
    public void setUp() {
        batch = new V1Batch();
    }

    @Test
    public void testIsEmpty() throws JsonProcessingException {
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message(1, MAPPER.writeValueAsBytes(new HashMap<>())));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() throws JsonProcessingException {
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, MAPPER.writeValueAsBytes(new HashMap<>())));
        assertEquals(1, batch.size());
    }

    @Test
    public void testGetProtocol() {
        assertEquals(Protocol.VERSION_1, batch.getProtocol());
    }

    @Test
    public void testCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent()
            throws JsonProcessingException {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(i, MAPPER.writeValueAsBytes(new HashMap<>())));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testCompleteBatchWithSequenceNumbersNotStartingAtOne()
            throws JsonProcessingException {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(
                    new Message(
                            startSequenceNumber + i, MAPPER.writeValueAsBytes(new HashMap<>())));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testHighSequence() throws JsonProcessingException {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(
                    new Message(
                            startSequenceNumber + i, MAPPER.writeValueAsBytes(new HashMap<>())));
        }

        assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
    }

    @Test
    public void testCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize()
            throws JsonProcessingException {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message(1, MAPPER.writeValueAsBytes(new HashMap<>())));

        assertFalse(batch.isComplete());
    }
}
