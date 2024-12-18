package com.pubnub.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

public interface PubNubKafkaRouter {
    ChannelAndMessage route(SinkRecord record);

    class ChannelAndMessage {
        private final String channel;
        private final Object message;

        public String getChannel() {
            return channel;
        }

        public Object getMessage() {
            return message;
        }

        public ChannelAndMessage(String channel, Object message) {
            this.channel = channel;
            this.message = message;
        }
    }
}
