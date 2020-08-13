package com.ebay.sojourner.business.ubd.detectors;

import com.ebay.sojourner.business.ubd.rule.RuleChangeEventListener;
import com.ebay.sojourner.business.ubd.rule.RuleManager;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.rule.RuleCategory;
import com.ebay.sojourner.common.model.rule.RuleChangeEvent;
import com.ebay.sojourner.common.model.rule.RuleDefinition;
import com.ebay.sojourner.dsl.sql.SQLEventRule;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBotDetector implements
    BotDetector<UbiEvent>, RuleChangeEventListener<RuleChangeEvent> {

  private List<SQLEventRule> sqlRules = new LinkedList<>();
  private final Set<Integer> botFlags = new HashSet<>();

  public EventBotDetector() {
    RuleManager ruleManager = RuleManager.getInstance();
    ruleManager.addListener(this);
    this.initBotRules();
  }

  @Override
  public Set<Integer> getBotFlagList(UbiEvent ubiEvent) {
    botFlags.clear();
    for (SQLEventRule rule : sqlRules) {
      int flag = rule.execute(ubiEvent);
      if (flag > 0) {
        botFlags.add(flag);
      }
    }

    return botFlags;
  }

  @Override
  public void initBotRules() {
    RuleManager ruleManager = RuleManager.getInstance();
    Set<RuleDefinition> rules = ruleManager.getEventRuleDefinitions();

    sqlRules = rules.stream()
        .map(SQLEventRule::new)
        .collect(Collectors.toList());
  }

  @Override
  public void onChange(RuleChangeEvent ruleChangeEvent) {
    sqlRules = ruleChangeEvent.getRules().stream()
        .map(SQLEventRule::new)
        .collect(Collectors.toList());
  }

  @Override
  public RuleCategory category() {
    return RuleCategory.EVENT;
  }
}
