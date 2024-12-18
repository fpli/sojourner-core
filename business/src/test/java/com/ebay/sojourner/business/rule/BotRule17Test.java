package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BotRule17Test extends BaseRulesTest<UbiSession> {

  private BotRule17 botRule17;
  private List<RulesTestCase> rulesTestCaseList;

  @BeforeEach
  public void setup() throws Exception {
    botRule17 = new BotRule17();
    rulesTestCaseList = loadTestCases("rule17.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() {
    return generateDynamicTests(rulesTestCaseList, botRule17);
  }
}
