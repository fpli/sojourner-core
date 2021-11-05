package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.IntegerField;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;

import java.util.HashMap;
import java.util.Map;

import com.ebay.sojourner.common.util.SojUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PageIdParser implements FieldParser<RawEvent, UbiEvent> {

    public void parse(RawEvent event, UbiEvent ubiEvent) {
        try {
            Map<String, String> map = new HashMap<>();
            map.putAll(event.getSojA());
            map.putAll(event.getSojK());
            map.putAll(event.getSojC());
            String pageid = null;
            if (StringUtils.isNotBlank(map.get(Constants.P_TAG))) {
                pageid = map.get(Constants.P_TAG);
            }
            String value = IntegerField.parse(pageid);
            if (ubiEvent.getGuid() != null && StringUtils.isNotBlank(pageid)) {
                ubiEvent.setPageId(Integer.parseInt(value));
                ubiEvent.setPageFamily(SojUtils.getPageFmly(Integer.parseInt(value)));
            }
        } catch (NumberFormatException e) {
            // log.warn("Parsing PageId failed, format incorrect...");
        }
    }

    @Override
    public void init() throws Exception {
        // nothing to do
    }
}
