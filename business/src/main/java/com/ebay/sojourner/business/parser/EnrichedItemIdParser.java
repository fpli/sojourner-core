package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.business.util.ItemIdExtractor;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

public class EnrichedItemIdParser implements FieldParser<RawEvent, UbiEvent> {

    @SneakyThrows
    public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
        if (ItemIdExtractor.accept(ubiEvent)) {
            String itemId = ItemIdExtractor.extractItemId(ubiEvent.getReferrer());
            if (StringUtils.isNotBlank(itemId)) {
                ubiEvent.setItemId(Long.parseLong(itemId));
            }
        }
    }

}
