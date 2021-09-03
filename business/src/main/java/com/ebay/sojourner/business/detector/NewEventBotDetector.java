package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.BotRule16;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.dsl.domain.rule.Rule;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class NewEventBotDetector implements BotDetector<UbiEvent> {

  private final Set<Rule<UbiEvent>> botRules = new LinkedHashSet<>();

  public NewEventBotDetector() {
    this.initBotRules();
  }

  @Override
  public Set<Integer> getBotFlagList(UbiEvent ubiEvent) throws IOException, InterruptedException {
    Set<Integer> botRuleList = new LinkedHashSet<>(botRules.size());
    for (Rule<UbiEvent> rule : botRules) {
      int botRule = rule.getBotFlag(ubiEvent);
      if (botRule != 0) {
        botRuleList.add(botRule);
      }
    }
    return botRuleList;
  }

  @Override
  public void initBotRules() {
    botRules.add(new BotRule16());
  }
}
