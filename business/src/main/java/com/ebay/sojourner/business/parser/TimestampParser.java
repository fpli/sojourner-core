package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.constant.SojHeaders;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.*;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TimestampParser implements FieldParser<RawEvent, UbiEvent> {
  private static final DateTimeFormatter formaterUtc =
          DateTimeFormat.forPattern(Constants.DEFAULT_TIMESTAMP_FORMAT)
                  .withZone(
                          DateTimeZone.forTimeZone(Constants.UTC_TIMEZONE));
  private static final DateTimeFormatter formater = DateTimeFormat.forPattern(
          Constants.DEFAULT_TIMESTAMP_FORMAT)
          .withZone(
                  DateTimeZone.forTimeZone(Constants.PST_TIMEZONE));
  private static final DateTimeFormatter dateMinsFormatter = DateTimeFormat.forPattern(
          Constants.DEFAULT_DATE_MINS_FORMAT)
          .withZone(
                  DateTimeZone.forTimeZone(Constants.PST_TIMEZONE));
  private static Set<Integer> appidWhiteList = Sets.newHashSet(35024,35023);
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Long eventTimestamp = rawEvent.getEventTimestamp();
    if(!appidWhiteList.contains(ubiEvent.getAppId())) {
      ubiEvent.setEventTimestamp(eventTimestamp);
    }else{
      parseTimestampForMobile(rawEvent,ubiEvent);
    }
    ubiEvent.setSojDataDt(SojTimestamp.castSojTimestampToDate(eventTimestamp));

    // Keep original session key from UBI Listener
    ubiEvent.setIngestTime(rawEvent.getIngestTime());
    Map<String, Long> timestamps = new HashMap<>();
    timestamps.put(SojHeaders.PATHFINDER_CREATE_TIMESTAMP,
        rawEvent.getRheosHeader().getEventCreateTimestamp());
    timestamps.put(SojHeaders.PATHFINDER_SENT_TIMESTAMP,
        rawEvent.getRheosHeader().getEventSentTimestamp());
    timestamps.put(SojHeaders.PATHFINDER_PRODUCER_TIMESTAMP,
        rawEvent.getTimestamps().get(SojHeaders.PATHFINDER_PRODUCER_TIMESTAMP));
    ubiEvent.setTimestamps(timestamps);
    ubiEvent.setOldSessionSkey(null);
  }

  @Override
  public void init() throws Exception {
  }

  private void parseTimestampForMobile(RawEvent rawEvent,UbiEvent ubiEvent){
    String applicationPayload = ubiEvent.getApplicationPayload();
    String mtstsString=null;
    Long eventTimestamp=null;
    StringBuilder buffer = new StringBuilder();
    if (!StringUtils.isBlank(applicationPayload)) {
      // get mtsts from payload
      mtstsString =
              SOJURLDecodeEscape.decodeEscapes(
                      SOJNVL.getTagValue(applicationPayload, Constants.TAG_MTSTS), '%');

      // compare ab_event_timestamp and mtsts
      if (!StringUtils.isBlank(mtstsString) && mtstsString.trim().length() >= 21) {
        buffer
                .append(mtstsString, 0, 10)
                .append(" ")
                .append(mtstsString, 11, 19)
                .append(".")
                .append(mtstsString.substring(20));
        mtstsString = buffer.toString();
        buffer.setLength(0);
        try {
          if (mtstsString.endsWith("Z") || mtstsString.contains("T")) {
            mtstsString = mtstsString.replaceAll("T", " ")
                    .replaceAll("Z", "");
            mtstsString= SojUtils.getTimestampStr(mtstsString);
            eventTimestamp = SojTimestamp.getSojTimestamp(
                    formaterUtc.parseDateTime(mtstsString).getMillis());
          } else {
            mtstsString= SojUtils.getTimestampStr(mtstsString);
            eventTimestamp = SojTimestamp
                    .getSojTimestamp(formater.parseDateTime(mtstsString).getMillis());
          }

        } catch (Exception e) {
          log.error("Invalid mtsts: " + mtstsString);
          eventTimestamp = rawEvent.getEventTimestamp();
        }
      } else {
        eventTimestamp = rawEvent.getEventTimestamp();
      }
    }
    ubiEvent.setEventTimestamp(eventTimestamp);
  }
}
