package com.ebay.sojourner.business.rule;

public abstract class AbstractBotRule<T> implements Rule<T> {

  @Override
  public void init() {
    // default empty implementation
  }
}
