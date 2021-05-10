package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CFLGS_TAG;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.FlagsUtils;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class CFlagFilterFunction extends RichFilterFunction<SojEvent> {

  @Override
  public boolean filter(SojEvent sojEvent) throws Exception {
    Map<String, String> applicationPayload = sojEvent.getApplicationPayload();
    return sojEvent.getPageId() != null
        && (applicationPayload == null
        || StringUtils.isBlank(applicationPayload.get(CFLGS_TAG))
        || !FlagsUtils.isBitSet(applicationPayload.get(CFLGS_TAG), 0));
  }
}
