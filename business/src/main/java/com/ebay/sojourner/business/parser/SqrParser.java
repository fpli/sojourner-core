package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.RegexReplace;
import com.ebay.sojourner.common.util.SOJURLDecodeEscape;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class SqrParser implements FieldParser<RawEvent, UbiEvent> {

  private static final String S_QR_TAG = "sQr";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Map<String, String> map = new HashMap<>();

    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());
    String sqr = null;
    if (StringUtils.isNotBlank(map.get(S_QR_TAG))) {
      sqr = map.get(S_QR_TAG);
    }
    try {
      if (StringUtils.isNotBlank(sqr)) {
        try {
          //different with jetstream, we will cut off when length exceed 4096,while jetstream not
          String sqrUtf8 = URLDecoder.decode(sqr, "UTF-8");
          if (sqrUtf8.length() <= 4096) {
            ubiEvent.setSqr(URLDecoder.decode(sqr, "UTF-8"));
          } else {
            ubiEvent.setSqr(URLDecoder.decode(sqr, "UTF-8").substring(0, 4096));
          }
        } catch (UnsupportedEncodingException e) {
          String replacedChar = RegexReplace
              .replace(sqr.replace('+', ' '), ".%[^0-9a-fA-F].?.", "", 1, 0, 'i');

          String replacedCharUtf8 = SOJURLDecodeEscape.decodeEscapes(replacedChar, '%');
          if (replacedCharUtf8.length() <= 4096) {
            ubiEvent.setSqr(SOJURLDecodeEscape.decodeEscapes(replacedChar, '%'));
          } else {
            ubiEvent.setSqr(SOJURLDecodeEscape.decodeEscapes(replacedChar, '%').substring(0, 4096));
          }
        }
      }
    } catch (Exception e) {
      // log.debug("Parsing Sqr failed, format incorrect: " + sqr);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
