package com.pubnub.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PubNubConnectorConfigTest {

    @Test
    public void basicParamsAreMandatory() {
        List<String> mandatoryParams = List.of("pubnub.user_id", "pubnub.subscribe_key", "pubnub.publish_key", "pubnub.secret_key");

        for (String value : mandatoryParams) {
            Map<String, String> props = mandatoryParams.stream().filter(s -> !s.equals(value)).collect(Collectors.toMap(s -> s, s -> "value"));
            assertThrows(ConfigException.class, () -> {
                new PubNubKafkaConnectorConfig(props);
            });
        }

    }
}
