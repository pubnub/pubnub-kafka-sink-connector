package com.pubnub.kafka.connect;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.PNCallback;
import com.pubnub.api.endpoints.pubsub.Publish;
import com.pubnub.api.models.consumer.PNErrorData;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PubNubKafkaSinkConnectorTaskTest {

    PubNub pubNub;
    PubNubKafkaSinkConnectorTask task;

    @BeforeEach
    public void before() {
        pubNub = mock();
        task = new PubNubKafkaSinkConnectorTask(pubNub, null);
    }

    @Test
    public void testPut() {
        String expectedTopic = "myTopic";
        Object expectedValue = new Object();
        String expectedTopic2 = "myTopic2";
        Object expectedValue2 = new Object();

        Publish publish = mock();
        Publish publish2 = mock();
        when(pubNub.publish()).thenReturn(publish, publish2);

        when(publish.channel(any())).thenReturn(publish);
        when(publish.message(any())).thenReturn(publish);

        when(publish2.channel(any())).thenReturn(publish2);
        when(publish2.message(any())).thenReturn(publish2);

        task.put(List.of(
                new SinkRecord(expectedTopic, 0, null, null, null, expectedValue, 0L),
                new SinkRecord(expectedTopic2, 0, null, null, null, expectedValue2, 0L)
        ));

        verify(publish).channel(expectedTopic);
        verify(publish).message(expectedValue);
        verify(publish).async(any());

        verify(publish2).channel(expectedTopic2);
        verify(publish2).message(expectedValue2);
        verify(publish2).async(any());
    }

    @Test
    public void testStart() {
        String expectedUserId = "user";
        String expectedSubscribeKey = "sub";
        String expectedPublishKey = "publish";
        String expectedSecretKey = "secret";

        HashMap<String, String> config = new HashMap<>();
        config.put("pubnub.user_id", expectedUserId);
        config.put("pubnub.subscribe_key", expectedSubscribeKey);
        config.put("pubnub.publish_key", expectedPublishKey);
        config.put("pubnub.secret_key", expectedSecretKey);
        config.put("pubnub.router", RouterImpl.class.getName());
        task.start(config);

        PNConfiguration pnConfig = task.getPubnub().getConfiguration();
        assertEquals(expectedUserId, pnConfig.getUuid());
        assertEquals(expectedSubscribeKey, pnConfig.getSubscribeKey());
        assertEquals(expectedPublishKey, pnConfig.getPublishKey());
        assertEquals(expectedSecretKey, pnConfig.getSecretKey());
        assertInstanceOf(RouterImpl.class, task.getRouter());
    }

    static public class RouterImpl implements PubNubKafkaRouter {
        @Override
        public ChannelAndMessage route(SinkRecord record) {
            Object message = record.value();
            if (message instanceof Map<?, ?>) {
                String channel = (String) ((Map<?, ?>) message).get("channel");
                Object value = (Object) ((Map<?, ?>) message).get("message");
                return new ChannelAndMessage(channel, value);
            }
            throw new RuntimeException("Invalid message");
        }
    }

    @Test
    public void testRouter() {
        task = new PubNubKafkaSinkConnectorTask(pubNub, new RouterImpl());
        String expectedTopic = "chan1";
        String expectedMessage = "some_message";
        Object expectedValue = new HashMap<String, Object>() {
            {
                put("channel", expectedTopic);
                put("message", expectedMessage);
            }
        };
        String expectedTopic2 = "chan2";
        String expectedMessage2 = "some_message2";
        Object expectedValue2 = new HashMap<String, Object>() {
            {
                put("channel", expectedTopic2);
                put("message", expectedMessage2);
            }
        };

        Publish publish = mock();
        Publish publish2 = mock();
        when(pubNub.publish()).thenReturn(publish, publish2);

        when(publish.channel(any())).thenReturn(publish);
        when(publish.message(any())).thenReturn(publish);

        when(publish2.channel(any())).thenReturn(publish2);
        when(publish2.message(any())).thenReturn(publish2);

        task.put(List.of(
                new SinkRecord("abc", 0, null, null, null, expectedValue, 0L),
                new SinkRecord("def", 0, null, null, null, expectedValue2, 0L)
        ));

        verify(publish).channel(expectedTopic);
        verify(publish).message(expectedMessage);
        verify(publish).async(any());

        verify(publish2).channel(expectedTopic2);
        verify(publish2).message(expectedMessage2);
        verify(publish2).async(any());
    }

    @Test
    public void testStop() {
        task.stop();

        verify(pubNub).destroy();
    }

    @Test
    public void testErrorReporter() {
        SinkRecord expectedRecord = new SinkRecord("myTopic", 0, null, null, null, new Object(), 0L);
        Exception expectedException = new RuntimeException("expected");
        Publish publish = mock();
        when(pubNub.publish()).thenReturn(publish);

        when(publish.channel(any())).thenReturn(publish);
        when(publish.message(any())).thenReturn(publish);
        ArgumentCaptor<PNCallback<PNPublishResult>> captor = ArgumentCaptor.captor();

        SinkTaskContext context = mock();
        ErrantRecordReporter reporter = mock();
        when(context.errantRecordReporter()).thenReturn(reporter);

        task.initialize(context);
        task.put(List.of(
                expectedRecord
        ));

        verify(publish).async(captor.capture());
        captor.getValue().onResponse(null,
                PNStatus.builder()
                        .error(true)
                        .errorData(new PNErrorData("error", expectedException))
                        .build());

        verify(reporter).report(expectedRecord, expectedException);
    }
}