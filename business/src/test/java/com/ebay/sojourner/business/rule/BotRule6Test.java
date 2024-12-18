package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.AgentAttribute;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BotRule6Test extends BaseRulesTest<AgentAttribute> {

  private BotRule6 botRule6;
  private List<RulesTestCase> rulesTestCaseList;

  @BeforeEach
  public void setup() throws Exception {
    botRule6 = new BotRule6();
    rulesTestCaseList = loadTestCases("rule6.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() {
    return generateDynamicTests(rulesTestCaseList, botRule6);
  }
}
