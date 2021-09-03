package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.BotRules;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;

public class BotRule16 extends AbstractBotRule<UbiEvent> {

  public static final Set<String> USER_ID_BLACK_LIST = Sets
      .newHashSet("1528692859", "1528686528", "2102965694", "1644547353", "1644548278");

  @Override
  public int getBotFlag(UbiEvent ubiEvent) throws IOException, InterruptedException {
    String userId = ubiEvent.getUserId();
    if (userId != null && USER_ID_BLACK_LIST.contains(userId)) {
      return BotRules.BLACK_USER_ID;
    }
    return 0;
  }
}
