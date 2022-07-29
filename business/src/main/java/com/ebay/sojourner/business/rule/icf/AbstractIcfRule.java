package com.ebay.sojourner.business.rule.icf;

import com.ebay.sojourner.business.rule.Rule;
import com.ebay.sojourner.common.util.BitUtils;
import com.ebay.sojourner.common.util.BotRules;

public abstract class AbstractIcfRule<T> implements Rule<T> {

  protected int getIcfRuleType(long icfBinary, int checkLocation) {
    if (BitUtils.isBitSet(icfBinary, checkLocation - 1)) {
      String format = String.format("%d%02d", BotRules.ICF_PREFIX, checkLocation);
      return Integer.parseInt(format);
    }
    return 0;
  }

  @Override
  public void init() {
    // default empty implementation
  }
}
