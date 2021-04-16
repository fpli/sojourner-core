package com.ebay.sojourner.business.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiSession;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class AgentIPMetricsTest extends BaseMetricsTest {

  private AgentIPMetrics agentIPMetrics;

  @BeforeEach
  public void setup() throws Exception {
    agentIPMetrics = new AgentIPMetrics();
    yaml = loadTestCasesYaml("AgentIPMetricsTest.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> test_feed() throws Exception {
    return generateDynamicTests(yaml, agentIPMetrics);
  }

  @Test
  void test_start() {
    UbiSession ubiSession = new UbiSession();
    ubiSession.setFindFirst(true);
    ubiSession.setInternalIp("internal IP");
    ubiSession.setExternalIp("external IP");
    ubiSession.setExternalIp2("external IP2");

    SessionAccumulator sessionAccumulator = new SessionAccumulator(ubiSession);
    agentIPMetrics.start(sessionAccumulator);

    assertThat(sessionAccumulator.getUbiSession().isFindFirst()).isFalse();
    assertThat(sessionAccumulator.getUbiSession().getInternalIp()).isNull();
    assertThat(sessionAccumulator.getUbiSession().getExternalIp()).isNull();
    assertThat(sessionAccumulator.getUbiSession().getExternalIp2()).isNull();
  }

  @Test
  void test_end() {
    UbiSession ubiSession = new UbiSession();
    ubiSession.setUserAgent("my-agent");
    ubiSession.setClientIp("1.1.1.1");
    ubiSession.setInternalIp("0.0.0.0");

    SessionAccumulator sessionAccumulator = new SessionAccumulator(ubiSession);
    agentIPMetrics.end(sessionAccumulator);

    assertThat(sessionAccumulator.getUbiSession().getUserAgent()).isEqualTo("my-agent");
    assertThat(sessionAccumulator.getUbiSession().getClientIp()).isEqualTo("1.1.1.1");
    assertThat(sessionAccumulator.getUbiSession().getExInternalIp()).isEqualTo("0.0.0.0");
  }
}
