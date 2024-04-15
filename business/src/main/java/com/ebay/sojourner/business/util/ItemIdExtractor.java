package com.ebay.sojourner.business.util;

import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJNVL;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.Set;

public class ItemIdExtractor {
    private static final String REGEX_STRING = ".*?/itm(/.*/|/)(\\d+).*";

    private static final Set<Integer> PAGES = ImmutableSet.of(2208336, 2356359);
    private static final String MOUDLE_ID = "mi%3A48379";
    private static final int DEWELL_THREDHOLD = 0;

    public static String extractItemId(String referrer) {
        if (StringUtils.isEmpty(referrer)) {
            return null;
        }
        if (referrer.matches(REGEX_STRING)) {
            return referrer.replaceAll(REGEX_STRING, "$2");
        }
        return null;
    }

    public static boolean accept(UbiEvent ubiEvent) {
        return ubiEvent.getItemId() == null && !ubiEvent.isRdt()
                && PAGES.contains(ubiEvent.getPageId())
                && MOUDLE_ID.equals(SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), "moduledtl"))
                && StringUtils.isNotEmpty(SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), "ex1"))
                && Long.parseLong(Optional.ofNullable(SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), "ex1"))
                .orElse("0")) > DEWELL_THREDHOLD;
    }

}
