package com.pubnub.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

public interface PubNubKafkaRouter {
    ChannelAndMessage route(SinkRecord record);

    class ChannelAndMessage {
        String channel;
        Object message;

        public ChannelAndMessage(String channel, Object message) {
            this.channel = channel;
            this.message = message;
        }
    }
}
