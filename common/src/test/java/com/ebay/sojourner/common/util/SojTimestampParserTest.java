package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.RheosHeader;
import org.assertj.core.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * first mtsts
 * if 1 min < the gap between mtsts and coalesce(TStamp,eventCreateTimestamp)
 * || the gap between mtsts and coalesce(TStamp,eventCreateTimestamp) < -30 mins
 * then coalesce(TStamp,eventCreateTimestamp) else mtsts
 */
public class SojTimestampParserTest {
    /**
     * TEST FOR STANDARD MILLISECONDS WITH UTC FLAG TIMESTAMP, WE USE UTC AS THE DEFAULT TIMEZONE
     */
    @Test
    public void standardUTCTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08T15:13:12.120Z"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // set to "2023-07-08T15:12:12.12Z"
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to mtsts value
        Assert.assertEquals(1688829192120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * TEST FOR NO MILLISECONDS WITH UTC FLAG TIMESTAMP, WE USE UTC AS THE DEFAULT TIMEZONE
     */
    @Test
    public void missMillsUTCTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08T15:13:12Z"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // set to "2023-07-08T15:12:12.12Z"
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to mtsts value
        Assert.assertEquals(1688829192000L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * TEST FOR NO MILLISECONDS WITH UTC FLAG AND DOT SEPERATOR TIMESTAMP, WE USE UTC AS THE DEFAULT TIMEZONE
     */
    @Test
    public void missMillswithDotUTCTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08T15:13:12.Z"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // set to "2023-07-08T15:12:12.12Z"
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to mtsts value
        Assert.assertEquals(1688829192000L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * TEST FOR PARTIAL MILLISECONDS WITH UTC FLAG TIMESTAMP, WE USE UTC AS THE DEFAULT TIMEZONE
     */
    @Test
    public void partialMillswithDotUTCTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08T15:13:12.12Z"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // set to "2023-07-08T15:12:12.12Z"
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to mtsts value
        Assert.assertEquals(1688829192120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * TEST FOR NON UTC FLAG TIMESTAMP, WE USE GMT-7 AS THE DEFAULT TIMEZONE
     */
    @Test
    public void noUTCFlagTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08 15:13:12.12"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // set to "2023-07-08T15:12:12.12Z"
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to TStamp value
        Assert.assertEquals(1688829132120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * use eventCreateTimestamp as the value due to Missed TStamp and Gap exceed the threshold
     */
    @Test
    public void noTStampTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08 15:13:12.12"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // not set TStamp
        //  clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to eventCreateTimestamp value
        Assert.assertEquals(1688829182120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * use eventCreateTimestamp as the value due to Missed TStamp
     * and page_id is 5660
     */
    @Test
    public void defaultValueTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08 15:13:12.12"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "5660"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // not set TStamp
        //  clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to eventCreateTimestamp value
        Assert.assertEquals(1688829182120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * use TStamp as the value
     * with page_id is 5660
     */
    @Test
    public void tstampValueTest() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08 15:13:12.12"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "5660"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // not set TStamp
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to eventCreateTimestamp value
        Assert.assertEquals(1688829132120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }

    /**
     * use TStamp as the value since mtsts is not mills based and no UTC flag
     */
    @Test
    public void tstampValueTest2() {
        RawEvent rawEvent = new RawEvent();
        rawEvent.setSojA(Maps.newHashMap("mtsts", "2023-07-08 15:13:12"));
        rawEvent.setSojC(new HashMap<>());
        rawEvent.setSojK(Maps.newHashMap("p", "123456"));
        // set eventCreatetimestamp to "2023-07-08T15:13:02.12Z"
        rawEvent.setRheosHeader(
                RheosHeader.newBuilder()
                        .setEventCreateTimestamp(1688829182120L)
                        .setEventSentTimestamp(1688829182120L)
                        .setSchemaId(111)
                        .setEventId("test")
                        .setProducerId("test")
                        .build());
        ClientData clientData = new ClientData();
        // not set TStamp
        clientData.setTStamp(String.valueOf(1688829132120L));
        rawEvent.setClientData(clientData);
        SojTimestampParser.parseEventTimestamp(rawEvent);
        //check if this is equal to eventCreateTimestamp value
        Assert.assertEquals(1688829132120L,
                SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp()).longValue());

    }
}
