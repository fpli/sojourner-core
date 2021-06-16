package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.SojHeaders.EP;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.ebay.sojourner.distributor.route.Route;
import java.nio.ByteBuffer;
import java.util.Map;

@Route(key = "entry-page")
public class EntryPageRouteRule extends AbstractSojEventRouteRule {

  @Override
  public boolean match(SojEvent sojEvent) {
    Map<String, ByteBuffer> sojHeader = sojEvent.getSojHeader();
    if (sojHeader != null && sojHeader.containsKey(EP)) {
      return ByteArrayUtils.toBoolean(sojHeader.get(EP).array());
    }
    return false;
  }
}
