package com.pubnub.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class PubNubKafkaConnectorConfig extends AbstractConfig {

    public PubNubKafkaConnectorConfig(final Map<?, ?> originalProps) {
        super(CONFIG_DEF, originalProps);
    }

    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addParams(configDef);
        return configDef;
    }

    private static void addParams(final ConfigDef configDef) {
        configDef.define(
                        "pubnub.user_id",
                        Type.STRING,
                        Importance.HIGH,
                        "The PubNub User ID")
                .define(
                        "pubnub.subscribe_key",
                        Type.STRING,
                        Importance.HIGH,
                        "The PubNub Subscribe API KEY")
                .define(
                        "pubnub.publish_key",
                        Type.STRING,
                        Importance.HIGH,
                        "The PubNub Publish API KEY")
                .define(
                        "pubnub.secret_key",
                        Type.PASSWORD,
                        Importance.HIGH,
                        "The PubNub Secret API KEY")
                .define(
                        "task.sleep.timeout",
                        Type.INT,
                        5000,
                        Importance.HIGH,
                        "Sleep timeout used by tasks during each poll");
    }

}
