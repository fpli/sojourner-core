package com.ebay.sojourner.business.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BuyerIdMetricsTest {

    private BuyerIdMetrics buyerIdMetrics;
    private SessionAccumulator sessionAccumulator;
    private UbiEvent ubiEvent;
    private UbiSession ubiSession;

    @BeforeEach
    public void setup() throws Exception {
        buyerIdMetrics = new BuyerIdMetrics();
        ubiSession = new UbiSession();
        sessionAccumulator = new SessionAccumulator(ubiSession);
        ubiEvent = mock(UbiEvent.class);
    }

    @Test
    void testBuyerIdMetrics() throws Exception {
        // Test 1: shouldSetBuyerIdWhenBuyerIdIsValidAndSessionBuyerIdIsNull
        when(ubiEvent.getApplicationPayload()).thenReturn("buyer_id=1234567890123456");
        when(ubiEvent.getEventTimestamp()).thenReturn(1000L);
        sessionAccumulator.getUbiSession().setAbsStartTimestamp(2000L);
        buyerIdMetrics.feed(ubiEvent, sessionAccumulator);
        assertThat(sessionAccumulator.getUbiSession().getBuyerId()).isEqualTo("1234567890123456");

        // Test 2: shouldNotSetBuyerIdWhenEventIsNotEarly
        when(ubiEvent.getApplicationPayload()).thenReturn("buyer_id=1234567890123458");
        when(ubiEvent.getEventTimestamp()).thenReturn(3000L);
        sessionAccumulator.getUbiSession().setAbsStartTimestamp(2000L);
        sessionAccumulator.getUbiSession().setBuyerId("123456");
        buyerIdMetrics.feed(ubiEvent, sessionAccumulator);
        assertThat(sessionAccumulator.getUbiSession().getBuyerId()).isEqualTo("123456");
    }
}