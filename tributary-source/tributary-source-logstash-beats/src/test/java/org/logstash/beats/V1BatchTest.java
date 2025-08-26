package org.logstash.beats;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.source.logstash.base.Message;

import java.security.SecureRandom;
import java.util.HashMap;

/** V1BatchTest. */
public class V1BatchTest {

    private V1Batch batch;

    @Before
    public void setUp() {
        batch = new V1Batch();
    }

    @Test
    public void testIsEmpty() {
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message<>(batch, 1, new HashMap<>()));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, batch.size());
        batch.addMessage(new Message<>(batch, 1, new HashMap<>()));
        assertEquals(1, batch.size());
    }

    @Test
    public void testGetProtocol() {
        assertEquals(Protocol.VERSION_1, batch.getProtocol());
    }

    @Test
    public void testCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message<>(batch, i, new HashMap<>()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testCompleteBatchWithSequenceNumbersNotStartingAtOne() {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message<>(batch, startSequenceNumber + i, new HashMap<>()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testHighSequence() {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message<>(batch, startSequenceNumber + i, new HashMap<>()));
        }

        assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
    }

    @Test
    public void testCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message<>(batch, 1, new HashMap<>()));

        assertFalse(batch.isComplete());
    }
}
