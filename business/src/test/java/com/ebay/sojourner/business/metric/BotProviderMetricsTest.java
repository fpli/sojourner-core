package com.ebay.sojourner.business.metric;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BotProviderMetricsTest extends BaseMetricsTest{
  private BotProviderMetrics botProviderMetrics;
  private JsonNode yaml;


  @BeforeEach
  public void setup() throws Exception {
    botProviderMetrics = new BotProviderMetrics();
    yaml = loadTestCasesYaml("BotProviderMetricsTest.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() throws Exception {
    return generateDynamicTests(yaml, botProviderMetrics);
  }
}
