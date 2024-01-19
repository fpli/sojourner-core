package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

public class SojUtilsTest {

    @Test
    public void isRover3084ClickReturnsTrueWhenPageIdIs3084() {
        UbiEvent ubiEvent = new UbiEvent();
        ubiEvent.setPageId(3084);
        Assertions.assertTrue(SojUtils.isRover3084Click(ubiEvent));
    }

    @Test
    public void isRover3084ClickReturnsFalseWhenPageIdIsNot3084() {
        UbiEvent ubiEvent = new UbiEvent();
        ubiEvent.setPageId(1234);
        Assertions.assertFalse(SojUtils.isRover3084Click(ubiEvent));
    }

    @Test
    public void extractSessionMetricsFromUbiSessionReturnsCorrectMetrics() {
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

        SessionMetrics sessionMetrics = SojUtils.extractSessionMetricsFromUbiSession(ubiSession);

        Assertions.assertEquals("test-guid", sessionMetrics.getGuid());
        Assertions.assertEquals("test-session-id", sessionMetrics.getSessionId());
        Assertions.assertEquals(123456L, sessionMetrics.getSessionSkey());
        Assertions.assertEquals(123456789L, sessionMetrics.getSojDataDt());
        Assertions.assertEquals(123456789L, sessionMetrics.getAbsEndTimestamp());
        Assertions.assertEquals(123456789L, sessionMetrics.getSessionEndDt());
        Assertions.assertEquals(-2208963476543L, sessionMetrics.getAbsStartTimestamp());
        Assertions.assertEquals(-2208963476543L, sessionMetrics.getSessionStartDt());
        Assertions.assertEquals(Arrays.asList(224, 225), sessionMetrics.getBotFlagList());
        Assertions.assertTrue(sessionMetrics.getIsOpen());
    }

    @Test
    public void checkFormatReturnsZeroWhenValueIsCorrect() {
        Assertions.assertEquals(0, SojUtils.checkFormat("Integer", "123"));
    }

    @Test
    public void checkFormatReturnsOneWhenValueIsIncorrect() {
        Assertions.assertEquals(1, SojUtils.checkFormat("Integer", "abc"));
    }
}
