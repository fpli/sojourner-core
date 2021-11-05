package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.IsValidIPv4;
import com.ebay.sojourner.common.util.IsValidPrivateIPv4;
import com.ebay.sojourner.common.util.SOJListGetValueByIndex;
import com.ebay.sojourner.common.util.SOJListLastElement;
import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ClientIPParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = Logger.getLogger(ClientIPParser.class);

  @Override
  public void init() throws Exception {
  }

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    ClientData clientData = rawEvent.getClientData();
    try {
      String remoteIP = clientData.getRemoteIP();
      String forwardedFor = clientData.getForwardFor();

      if (StringUtils.isNotBlank(remoteIP)) {
        if (IsValidIPv4.isValidIP(remoteIP) && !IsValidPrivateIPv4.isValidIP(remoteIP)) {
          ubiEvent.setClientIP(remoteIP);
        } else {
          String forwardValueByIndex =
              SOJListGetValueByIndex.getValueByIndex(
                  SOJListLastElement.getLastElement(forwardedFor, ":"), ",", 1);
          if (IsValidIPv4.isValidIP(forwardValueByIndex)
              && !IsValidPrivateIPv4.isValidIP(forwardValueByIndex)) {
            ubiEvent.setClientIP(forwardValueByIndex);
          } else if (IsValidIPv4.isValidIP(remoteIP)) {
            ubiEvent.setClientIP(remoteIP);
          }
        }
      }
    } catch (Exception e) {
      // log.debug("Parsing ClientIP failed, format incorrect: " + clientData);
    }
  }
}
