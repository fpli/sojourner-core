package com.ebay.sojourner.ubd.common.rule;

import com.ebay.sojourner.ubd.common.model.UbiSession;
import com.ebay.sojourner.ubd.common.util.BotRules;
import com.ebay.sojourner.ubd.common.util.Property;
import com.ebay.sojourner.ubd.common.util.PropertyUtils;
import com.ebay.sojourner.ubd.common.util.UBIConfig;

import java.util.Set;

public class BotRule208 implements Rule<UbiSession> {

    private static Set<Integer> cobrandSet;

    @Override
    public void init() {
        cobrandSet = PropertyUtils.getIntegerSet(UBIConfig.getString(Property.EBAY_SITE_COBRAND), Property.PROPERTY_DELIMITER);
    }

    @Override
    public int getBotFlag(UbiSession session) {
        if (session.getAgentString() == null && session.getSiidCnt() == 0 && !cobrandSet.contains(session.getCobrand())) {
            return BotRules.DIRECT_ACCESS_BOTFLAG;
        }
        return 0;
    }

}
