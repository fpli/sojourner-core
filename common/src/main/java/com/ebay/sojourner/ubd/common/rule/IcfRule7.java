package com.ebay.sojourner.ubd.common.rule;

import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.util.IcfRuleUtils;

import java.io.IOException;

public class IcfRule7 implements Rule<UbiEvent> {
    @Override
    public void init() {

    }

    @Override
    public int getBotFlag(UbiEvent ubiEvent) throws IOException, InterruptedException {
        return IcfRuleUtils.getIcfRuleType(ubiEvent.getApplicationPayload());
    }
}
