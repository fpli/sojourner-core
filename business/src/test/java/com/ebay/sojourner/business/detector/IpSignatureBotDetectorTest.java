package com.ebay.sojourner.business.detector;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.business.rule.Rule;
import com.ebay.sojourner.common.model.IpAttribute;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

class IpSignatureBotDetectorTest {

  IpSignatureBotDetector ipSignatureBotDetector;

  @BeforeEach
  void setUp() {
    ipSignatureBotDetector = IpSignatureBotDetector.getInstance();
  }

  @Test
  void getBotFlagList() throws Exception {
    IpAttribute ipAttribute = new IpAttribute();
    ipAttribute.setScsCount(20);

    Set<Integer> result = ipSignatureBotDetector.getBotFlagList(ipAttribute);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(7)).isTrue();
  }

  @Test
  void initBotRules() {
    Set<Rule> botRules = Whitebox.getInternalState(ipSignatureBotDetector, "botRules", IpSignatureBotDetector.class);
    assertThat(botRules.size()).isEqualTo(2);
  }
}