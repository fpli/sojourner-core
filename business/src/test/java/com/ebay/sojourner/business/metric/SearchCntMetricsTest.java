package com.ebay.sojourner.business.metric;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class SearchCntMetricsTest extends BaseMetricsTest {

  private SearchCntMetrics searchCntMetrics;
  private JsonNode yaml;

  @BeforeEach
  public void setup() throws Exception {
    searchCntMetrics = new SearchCntMetrics();
    yaml = loadTestCasesYaml("SearchCntMetricsTest.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() throws Exception {
    return generateDynamicTests(yaml, searchCntMetrics);
  }
}
