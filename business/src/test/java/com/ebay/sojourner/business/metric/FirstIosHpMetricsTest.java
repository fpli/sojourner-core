package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FirstIosHpMetricsTest {

    private FirstIosHpMetrics firstIosHpMetrics;
    private SessionAccumulator sessionAccumulator;
    private UbiEvent ubiEvent;

    @BeforeEach
    public void setup() throws Exception {
        firstIosHpMetrics = new FirstIosHpMetrics();
        sessionAccumulator = new SessionAccumulator(new UbiSession());
        ubiEvent = mock(UbiEvent.class);
    }

    @Test
    void firstIosHpMetricsTests() throws Exception {
        // Test 1: shouldSetFirstIosFgLaunchTimestampWhenIosFgEventIsEarly
        when(ubiEvent.getPageId()).thenReturn(2051248);
        when(ubiEvent.getEventTimestamp()).thenReturn(1000L);
        when(ubiEvent.getApplicationPayload()).thenReturn("app=1462");
        firstIosHpMetrics.feed(ubiEvent, sessionAccumulator);
        assertEquals(1000L, sessionAccumulator.getUbiSession().getFirstIosFgLaunchTimestamp());

    }
}