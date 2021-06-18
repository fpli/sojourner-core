package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleDistSojEventPartitioner {

  private static final String FIELD_DELIM = ",";

  public static Integer hash(SimpleDistSojEventWrapper simpleDistSojEventWrapper,
      List<String> keyList) {

    if (simpleDistSojEventWrapper == null) {
      return null;
    } else {
      Field field = null;
      List<String> valueList = new ArrayList<>();
      for (String keyName : keyList) {
        try {
          field = simpleDistSojEventWrapper.getClass().getDeclaredField(keyName);
          field.setAccessible(true);
          Object value = field.get(simpleDistSojEventWrapper);
          valueList.add(String.valueOf(value));
        } catch (Exception e) {
          log.error("Get field[{}] value error", keyName, e);
        }
      }

      String hashKey = String.join(FIELD_DELIM, valueList);
      return hashKey.hashCode();
    }
  }
}
