package com.ebay.sojourner.business.ubd.rule;

import com.ebay.sojourner.common.model.rule.Rule;

public abstract class AbstractBotRule<T> implements Rule<T> {

  @Override
  public void init() {
    // default empty implementation
  }
}