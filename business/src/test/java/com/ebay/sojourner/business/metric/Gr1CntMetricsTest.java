package com.ebay.sojourner.business.metric;

import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class Gr1CntMetricsTest extends BaseMetricsTest {

  private Gr1CntMetrics gr1CntMetrics;

  @BeforeEach
  public void setup() throws Exception {
    gr1CntMetrics = new Gr1CntMetrics();
    yaml = loadTestCasesYaml("Gr1CntMetricsTest.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() throws Exception {
    return generateDynamicTests(yaml, gr1CntMetrics);
  }
}
