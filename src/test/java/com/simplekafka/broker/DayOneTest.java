package com.simplekafka.broker;

import org.junit.jupiter.api.Test;

final class DayOneTest {
    @Test
    void protocolAndBrokerMetadataChecksPass() {
        DayOneChecks.main(new String[0]);
    }
}
