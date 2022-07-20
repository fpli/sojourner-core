package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BotRule21Test extends BaseRulesTest<AgentIpAttribute> {

  private BotRule21 botRule21;
  private List<RulesTestCase> rulesTestCaseList;

  @BeforeEach
  public void setup() throws Exception {
    botRule21 = new BotRule21();
    rulesTestCaseList = loadTestCases("rule21.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() {
    return generateDynamicTests(rulesTestCaseList, botRule21);
  }
}
