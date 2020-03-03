package com.ebay.sojourner.ubd.common.sharedlib.parser;

import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class SqrParser implements FieldParser<RawEvent, UbiEvent> {
  private static final Logger log = Logger.getLogger(SqrParser.class);
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
        ubiEvent.setSqr(sqr);
        //                try{
        //                    String sqrUtf8 = URLDecoder.decode(sqr, "UTF-8");
        //                    if(sqrUtf8.length() <= 4096){
        //                        ubiEvent.setSqr(URLDecoder.decode(sqr,"UTF-8"));
        //                    } else {
        //                        ubiEvent.setSqr(URLDecoder.decode(sqr,"UTF-8").substring(0,4096));
        //                    }
        //
        //                }catch (UnsupportedEncodingException e){
        //                    String replacedChar = RegexReplace.replace(sqr.replace('+', '
        // '),".%[^0-9a-fA-F].?.", "", 1, 0, 'i');
        //
        //                    String replacedCharUtf8 =
        // SOJURLDecodeEscape.decodeEscapes(replacedChar, '%');
        //                    if(replacedCharUtf8.length() <= 4096){
        //                        ubiEvent.setSqr(SOJURLDecodeEscape.decodeEscapes(replacedChar,
        // '%'));
        //                    }else {
        //                        ubiEvent.setSqr(SOJURLDecodeEscape.decodeEscapes(replacedChar,
        // '%').substring(0,4096));
        //                    }
        //
        //                }
      }
    } catch (Exception e) {
      log.debug("Parsing Sqr failed, format incorrect: " + sqr);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
