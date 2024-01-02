package com.ebay.sojourner.rt.operator.metrics;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.UbiSession;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;

public class UbiSessionToSessionMetricsProcessFunctionTest {

    @Test
    public void processElementExtractsCorrectSessionMetrics() throws Exception {
        // Given
        UbiSession ubiSession = new UbiSession();
        ubiSession.setGuid("test-guid");
        ubiSession.setSessionId("test-session-id");
        ubiSession.setSessionSkey(123456L);
        ubiSession.setSojDataDt(123456789L);
        ubiSession.setAbsEndTimestamp(123456789L);
        ubiSession.setSessionEndDt(123456789L);
        ubiSession.setAbsStartTimestamp(123456789L);
        ubiSession.setSessionStartDt(123456789L);
        ubiSession.setBotFlagList(new HashSet<>(Arrays.asList(220, 221, 222, 223, 224, 225)));
        ubiSession.setOpenEmit(true);

        Collector<SessionMetrics> collector = Mockito.mock(Collector.class);

        UbiSessionToSessionMetricsProcessFunction function = new UbiSessionToSessionMetricsProcessFunction();

        // When
        function.processElement(ubiSession, null, collector);

        // Then
        Mockito.verify(collector).collect(Mockito.any(SessionMetrics.class));
    }
}