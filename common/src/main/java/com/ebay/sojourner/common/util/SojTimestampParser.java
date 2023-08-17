package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.RawEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.net.URLDecoder;
import java.text.ParseException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
@Slf4j
public class SojTimestampParser {
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
    public static void parseEventTimestamp(RawEvent rawEvent) {
        StringBuilder buffer = new StringBuilder();
        Long abEventTimestamp;
        Long eventTimestamp;
        Long interval;
        String mtstsString;
        String pageId = null;
        Map<String, String> map = new HashMap<>();
        map.putAll(rawEvent.getSojA());
        map.putAll(rawEvent.getSojK());
        map.putAll(rawEvent.getSojC());
        String applicationPayload = null;
        String mARecString = PropertyUtils.mapToString(rawEvent.getSojA());
        String mKRecString = PropertyUtils.mapToString(rawEvent.getSojK());
        String mCRecString = PropertyUtils.mapToString(rawEvent.getSojC());
        if (mARecString != null) {
            applicationPayload = mARecString;
        }
        if ((applicationPayload != null) && (mKRecString != null)) {
            applicationPayload = applicationPayload + "&" + mKRecString;
        }

        // else set C record
        if (applicationPayload == null) {
            applicationPayload = mCRecString;
        }
        Long origEventTimeStamp = rawEvent.getRheosHeader().getEventCreateTimestamp();
        abEventTimestamp = SojTimestamp.getSojTimestamp(origEventTimeStamp);

        // for cal2.0 abeventtimestamp format change(from soj timestamp to EPOCH timestamp)
        String tstamp = rawEvent.getClientData().getTStamp();
        if (tstamp != null) {
            try {
                abEventTimestamp = Long.valueOf(rawEvent.getClientData().getTStamp());
                abEventTimestamp = SojTimestamp.getSojTimestamp(abEventTimestamp);
            } catch (NumberFormatException e) {
                abEventTimestamp = SojTimestamp.getSojTimestamp(origEventTimeStamp);
            }
        }

        if (StringUtils.isNotBlank(map.get(Constants.P_TAG))) {
            pageId = map.get(Constants.P_TAG);
        }

        if (pageId != null && !pageId.equals("5660")) {
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
                    String mtstsStrTmp = buffer.toString();
                    buffer.setLength(0);
                    try {
                        if (mtstsStrTmp.endsWith("Z") || mtstsStrTmp.contains("T")) {
                            mtstsStrTmp = mtstsStrTmp.replaceAll("T", " ")
                                    .replaceAll("Z", "");
                            eventTimestamp =
                                    SojTimestamp.getSojTimestamp(formaterUtc.parseDateTime(mtstsStrTmp).getMillis());
                        } else {
                            eventTimestamp = SojTimestamp
                                    .getSojTimestamp(formater.parseDateTime(mtstsStrTmp).getMillis());
                        }
                        interval = getMicroSecondInterval(eventTimestamp, abEventTimestamp);
                        if (interval > Constants.UPPERLIMITMICRO || interval < Constants.LOWERLIMITMICRO) {
                            eventTimestamp = abEventTimestamp;
                        }
                    } catch (Exception e) {
                        log.error("Invalid mtsts: {}, " , mtstsString,e);
                        if(mtstsString.endsWith("Z") && mtstsString.contains("T"))
                        {
                            eventTimestamp = getUTCMillSeconds(mtstsString, abEventTimestamp);
                        } else {
                            eventTimestamp = abEventTimestamp;
                        }
                    }
                } else {
                    if(StringUtils.isNotBlank(mtstsString)&&mtstsString.endsWith("Z") && mtstsString.contains("T"))
                    {
                        eventTimestamp = getUTCMillSeconds(mtstsString, abEventTimestamp);
                    } else {
                        eventTimestamp = abEventTimestamp;
                    }
                }
            } else {
                eventTimestamp = abEventTimestamp;
            }
        } else {
            eventTimestamp = abEventTimestamp;
        }
        rawEvent.setEventTimestamp(eventTimestamp);
    }

    private static Long getMicroSecondInterval(Long microts1, Long microts2) throws ParseException {
        Long v1, v2;
        v1 = dateMinsFormatter.parseDateTime(dateMinsFormatter.print(microts1 / 1000)).getMillis();
        v2 = dateMinsFormatter.parseDateTime(dateMinsFormatter.print(microts2 / 1000)).getMillis();
        return (v1 - v2) * 1000;
    }

    private static Long getUTCMillSeconds(String mtstsString,long abEventTimestamp)
    {
        try {
            long eventTimestamp = SojTimestamp
                    .getSojTimestamp(OffsetDateTime
                            .parse(URLDecoder.decode(mtstsString, "utf-8"),
                                    java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
                                            .withZone(ZoneOffset.UTC)).toInstant().toEpochMilli());
            long interval = getMicroSecondInterval(eventTimestamp, abEventTimestamp);
            if (interval > Constants.UPPERLIMITMICRO || interval < Constants.LOWERLIMITMICRO) {
                return abEventTimestamp;
            }else{
                return eventTimestamp;
            }
        }catch (Exception ee){
            log.error("Invalid mtsts when using OffsetDateTime : {}, " , mtstsString,ee);
            return abEventTimestamp;
        }
    }
}
