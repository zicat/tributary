package org.logstash.beats;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.source.logstash.beats.BatchMessageListener;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/** Created by ph on 2016-06-01. */
public class BeatsHandlerTest {
    private static final SecureRandom randomizer = new SecureRandom();
    private SpyListener spyListener;
    private final int startSequenceNumber = randomizer.nextInt(100);
    private final int messageCount = 5;
    private V1Batch batch;
    public static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new AfterburnerModule());

    /** SpyListener. */
    private static class SpyListener implements BatchMessageListener {
        private final List<Message> lastMessages = new ArrayList<Message>();

        @Override
        public void consume(Iterator<Message> iterator) {
            while (iterator.hasNext()) {
                Message message = iterator.next();
                lastMessages.add(message);
            }
        }

        public List<Message> getLastMessages() {
            return lastMessages;
        }
    }

    @Before
    public void setup() throws JsonProcessingException {
        spyListener = new SpyListener();
        batch = new V1Batch();
        batch.setBatchSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            Message message =
                    new Message(i + startSequenceNumber, MAPPER.writeValueAsBytes(new HashMap<>()));
            batch.addMessage(message);
        }
    }

    @Test
    public void testItCalledOnNewConnectionOnListenerWhenHandlerIsAdded() {
        EmbeddedChannel embeddedChannel =
                new EmbeddedChannel(new ConnectionHandler(), new BeatsHandler(spyListener));
        embeddedChannel.pipeline().fireChannelActive();
        embeddedChannel.writeInbound(batch);
        embeddedChannel.close();
    }

    @Test
    public void testItCalledOnConnectionCloseOnListenerWhenChannelIsRemoved() {
        EmbeddedChannel embeddedChannel =
                new EmbeddedChannel(new ConnectionHandler(), new BeatsHandler(spyListener));
        embeddedChannel.pipeline().fireChannelActive();
        embeddedChannel.writeInbound(batch);
        embeddedChannel.close();
    }

    @Test
    public void testIsCallingNewMessageOnEveryMessage() {
        EmbeddedChannel embeddedChannel =
                new EmbeddedChannel(new ConnectionHandler(), new BeatsHandler(spyListener));
        embeddedChannel.pipeline().fireChannelActive();
        embeddedChannel.writeInbound(batch);

        assertEquals(messageCount, spyListener.getLastMessages().size());
        embeddedChannel.close();
    }

    @Test
    public void testAcksLastMessageInBatch() {
        EmbeddedChannel embeddedChannel =
                new EmbeddedChannel(new ConnectionHandler(), new BeatsHandler(spyListener));
        embeddedChannel.pipeline().fireChannelActive();
        embeddedChannel.writeInbound(batch);
        assertEquals(messageCount, spyListener.getLastMessages().size());
        Ack ack = embeddedChannel.readOutbound();
        assertEquals(Protocol.VERSION_1, ack.getProtocol());
        assertEquals(startSequenceNumber + messageCount - 1, ack.getSequence());
        embeddedChannel.close();
    }

    @Test
    public void testAcksZeroSequenceForEmptyBatch() {
        EmbeddedChannel embeddedChannel =
                new EmbeddedChannel(new ConnectionHandler(), new BeatsHandler(spyListener));
        embeddedChannel.pipeline().fireChannelActive();
        embeddedChannel.writeInbound(new V2Batch());
        assertEquals(0, spyListener.getLastMessages().size());
        Ack ack = embeddedChannel.readOutbound();
        assertEquals(Protocol.VERSION_2, ack.getProtocol());
        assertEquals(0, ack.getSequence());
        embeddedChannel.close();
    }
}
