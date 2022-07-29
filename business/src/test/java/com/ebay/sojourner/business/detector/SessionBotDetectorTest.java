package com.ebay.sojourner.business.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ebay.sojourner.business.rule.*;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.UbiBotFilter;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SessionBotDetector.class)
public class SessionBotDetectorTest {

  SessionBotDetector sessionBotDetector;

  UbiBotFilter mockBotFilter = mock(UbiBotFilter.class);
  BotRule1 mockRule1 = mock(BotRule1.class);
  BotRule15 mockRule15 = mock(BotRule15.class);
  BotRule9 mockRule9 = mock(BotRule9.class);
  BotRule10 mockRule10 = mock(BotRule10.class);
  BotRule12 mockRule12 = mock(BotRule12.class);
  BotRule203 mockRule203 = mock(BotRule203.class);
  BotRule204 mockRule204 = mock(BotRule204.class);
  BotRule205 mockRule205 = mock(BotRule205.class);
  BotRule207 mockRule207 = mock(BotRule207.class);
  BotRule212 mockRule212 = mock(BotRule212.class);
  BotRule215 mockRule215 = mock(BotRule215.class);
  BotRule11 mockRule11 = mock(BotRule11.class);
  BotRule17 mockRule17 = mock(BotRule17.class);


  @Before
  public void setUp() throws Exception {
    PowerMockito.whenNew(UbiBotFilter.class).withNoArguments().thenReturn(mockBotFilter);
    PowerMockito.whenNew(BotRule1.class).withNoArguments().thenReturn(mockRule1);
    PowerMockito.whenNew(BotRule15.class).withNoArguments().thenReturn(mockRule15);
    PowerMockito.whenNew(BotRule9.class).withNoArguments().thenReturn(mockRule9);
    PowerMockito.whenNew(BotRule10.class).withNoArguments().thenReturn(mockRule10);
    PowerMockito.whenNew(BotRule12.class).withNoArguments().thenReturn(mockRule12);
    PowerMockito.whenNew(BotRule203.class).withNoArguments().thenReturn(mockRule203);
    PowerMockito.whenNew(BotRule204.class).withNoArguments().thenReturn(mockRule204);
    PowerMockito.whenNew(BotRule205.class).withNoArguments().thenReturn(mockRule205);
    PowerMockito.whenNew(BotRule207.class).withNoArguments().thenReturn(mockRule207);
    PowerMockito.whenNew(BotRule212.class).withNoArguments().thenReturn(mockRule212);
    PowerMockito.whenNew(BotRule215.class).withNoArguments().thenReturn(mockRule215);
    PowerMockito.whenNew(BotRule11.class).withNoArguments().thenReturn(mockRule11);
    PowerMockito.whenNew(BotRule17.class).withNoArguments().thenReturn(mockRule17);
    when(mockBotFilter.filter(any(), any())).thenReturn(false);
    when(mockRule1.getBotFlag(any())).thenReturn(1);
    sessionBotDetector = SessionBotDetector.getInstance();
  }

  @Test
  public void getBotFlagList() throws Exception {
    UbiSession ubiSession = new UbiSession();
    ubiSession.setAgentInfo("bot");

    Set<Integer> result = sessionBotDetector.getBotFlagList(ubiSession);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(1)).isTrue();
  }

  @Test
  public void initBotRules() {
    Set<Rule> botRules = Whitebox
        .getInternalState(sessionBotDetector, "botRules", SessionBotDetector.class);
    assertThat(botRules.size()).isEqualTo(12);
  }
}