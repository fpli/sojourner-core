package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BotRule18Test extends BaseRulesTest<UbiSession> {

  private BotRule18 botRule18;
  private List<RulesTestCase> rulesTestCaseList;

  @BeforeEach
  public void setup() throws Exception {
    botRule18 = new BotRule18();
    rulesTestCaseList = loadTestCases("rule18.yaml");
  }

  @TestFactory
  public Collection<DynamicTest> dynamicTests() {
    return generateDynamicTests(rulesTestCaseList, botRule18);
  }
}
