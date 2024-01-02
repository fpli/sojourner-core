package com.ebay.sojourner.business.util;

import com.ebay.sojourner.common.model.RawEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class LoadRawEventAndExpect {

  private static RawEvent rawEvent = null;
  private static Object expectResult = null;

  public static HashMap<RawEvent, Object> getRawEventAndExpect(
      Map<String, Object> map, String parser, String caseItem) {

    if (StringUtils.isBlank(parser) & StringUtils.isBlank(caseItem)) {
      log.error("the parser or caseItem is blank!!!");
    }

    if (map.isEmpty()) {
      log.error("the map is empty!!!");
    }

    HashMap<RawEvent, Object> hashMap = new HashMap<>();

    HashMap<String, Object> map1 = TypeTransUtil.ObjectToHashMap(map.get(parser));
    HashMap<String, Object> map2 = TypeTransUtil.ObjectToHashMap(map1.get(caseItem));

    rawEvent = InitRawEvent.initRawEvent(map2, parser);
    expectResult = map2.get(ParserConstants.EXPECTRESULT);

    hashMap.put(rawEvent, expectResult);

    return hashMap;
  }
}
