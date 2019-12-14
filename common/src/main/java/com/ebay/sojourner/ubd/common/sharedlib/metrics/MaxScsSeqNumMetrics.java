package com.ebay.sojourner.ubd.common.sharedlib.metrics;

import com.ebay.sojourner.ubd.common.model.SessionAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.util.Property;
import com.ebay.sojourner.ubd.common.util.PropertyUtils;
import com.ebay.sojourner.ubd.common.util.UBIConfig;

import java.io.File;
import java.io.InputStream;
import java.util.Set;

public class MaxScsSeqNumMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

    private static UBIConfig ubiConfig;
    private static Set<Integer> invalidPageIds;

    @Override
    public void start(SessionAccumulator sessionAccumulator) {
        sessionAccumulator.getUbiSession().setMaxScsSeqNum(Integer.MIN_VALUE);

    }

    @Override
    public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
        if (event.getIframe() == 0 && event.getRdt() == 0 && !invalidPageIds.contains(event.getPageId())) {
            if (event.getSeqNum() > sessionAccumulator.getUbiSession().getMaxScsSeqNum()) {
                sessionAccumulator.getUbiSession().setMaxScsSeqNum(event.getSeqNum());
            }
        }
    }

    @Override
    public void end(SessionAccumulator sessionAccumulator) {

    }

    @Override
    public void init() throws Exception {
        InputStream resourceAsStream = MaxScsSeqNumMetrics.class.getResourceAsStream("/ubi.properties");
        ubiConfig = UBIConfig.getInstance(resourceAsStream);
        invalidPageIds = PropertyUtils.getIntegerSet(ubiConfig.getString(Property.INVALID_PAGE_IDS), Property.PROPERTY_DELIMITER);
    }
}