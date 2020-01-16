package com.ebay.sojourner.ubd.rt.operators.event;

import com.ebay.sojourner.ubd.common.model.ClientData;
import com.ebay.sojourner.ubd.common.model.RheosHeader;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.sharedlib.parser.LkpFetcher;
import com.ebay.sojourner.ubd.common.sharedlib.util.IntegerField;
import com.ebay.sojourner.ubd.common.sharedlib.util.SOJTS2Date;
import com.ebay.sojourner.ubd.common.util.PropertyUtils;
import com.ebay.sojourner.ubd.rt.connectors.kafka.RheosEventSerdeFactory;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class EventDeserializeMapFunction extends RichMapFunction<byte[], UbiEvent> {
    private static final Logger logger = Logger.getLogger(EventDeserializeMapFunction.class);

    @Override
    public UbiEvent map( byte[] rawEvent ) throws Exception {
        RheosEvent rheosEvent = RheosEventSerdeFactory.getRheosEventHeaderDeserializer()
                .deserialize(null, rawEvent);
        System.out.println(rheosEvent.getEventCreateTimestamp());
        GenericRecord genericRecord = RheosEventSerdeFactory.getRheosEventDeserializer()
                .decode(rheosEvent);

        // Generate RheosHeader
        RheosHeader rheosHeader = new RheosHeader(rheosEvent.getEventCreateTimestamp(),
                rheosEvent.getEventSentTimestamp(),
                rheosEvent.getSchemaId(),
                rheosEvent.getEventId(),
                rheosEvent.getProducerId()
        );

        Map<String, String> sojAMap = new HashMap<>();

        Map<String, String> sojKMap = new HashMap<>();


        Map<String, String> sojCMap = new HashMap<>();

        // Generate ClientData
        // If clientData is not of type GenericRecord, just skip this message.
        if (!(genericRecord.get("clientData") instanceof Map)) {
            logger.info("clientData is not of type Map. "
                    + genericRecord.get("clientData"));
            return null;
        }
//        String appid=getString2(genericRecord.get("appId"));


//        GenericRecord applicationPayload =  genericRecord.get("applicationPayload");
        Map<Utf8, Utf8> applicationPayloadUtf8 = (Map<Utf8, Utf8>) genericRecord.get("applicationPayload");
        Map<String, String> applicationPayload = new HashMap<>();
        if (applicationPayload != null) {
            for (Map.Entry<Utf8, Utf8> entry : applicationPayloadUtf8.entrySet()) {
                applicationPayload.put(getString(entry.getKey()), getString(entry.getValue().toString()));
            }
        }
//        for(Map.Entry<String,String> entry:fieldMap.entrySet())
//        {
//            String key=entry.getKey();
//            String value=entry.getValue();
//            String valueMid=getString(genericRecord.get(key));
//            applicationPayload.put(value, valueMid);
//
//        }
        sojAMap.putAll(applicationPayload);
        String agentFromAP = getString2(applicationPayload.get("Agent"));
        String agent = getString2(genericRecord.get("agentInfo"));
        String agentCLI = agent.length() > agentFromAP.length() ? agent : agentFromAP;
        Map<Utf8, Utf8> clientDataMapUtf8 = (Map<Utf8, Utf8>) genericRecord.get("clientData");
        Map<String, String> clientDataMap = new HashMap<>();
        if (clientDataMap != null) {
            for (Map.Entry<Utf8, Utf8> entry : clientDataMapUtf8.entrySet()) {
                clientDataMap.put(getString(entry.getKey()), getString(entry.getValue().toString()));
            }
        }

        ClientData clientData = new ClientData();
        clientData.setForwardFor(getString(clientDataMap.get("ForwardFor")));
        clientData.setScript(getString(clientDataMap.get("Script")));
        clientData.setServer(getString(clientDataMap.get("Server")));
        clientData.setTMachine(getString(clientDataMap.get("TMachine")));

        clientData.setTStamp(StringUtils.isEmpty(applicationPayload.get("timestamp")) ? null : Long.valueOf(getString(applicationPayload.get("timestamp"))));
        clientData.setTName(getString(clientDataMap.get("TName")));
        clientData.setTPayload(getString(clientDataMap.get("TPayload")));
        clientData.setColo(getString(applicationPayload.get("colo")));
        clientData.setPool(getString(applicationPayload.get("TPool")));

        clientData.setTType(getString(applicationPayload.get("TType")));
        clientData.setTStatus(getString(clientDataMap.get("TStatus")));
        clientData.setCorrId(getString(applicationPayload.get("corrId")));
        clientData.setContentLength(StringUtils.isEmpty(clientDataMap.get("ContentLength")) ? null : Integer.valueOf(getString(clientDataMap.get("ContentLength"))));
        clientData.setNodeId(getString(applicationPayload.get("nodeId")));
        clientData.setRequestGuid(getString(applicationPayload.get("requestGuid")));

        clientData.setReferrer(getString(applicationPayload.get("Referer")));

        clientData.setAcceptEncoding(getString(clientDataMap.get("Encoding")));
        clientData.setTDuration(StringUtils.isEmpty(clientDataMap.get("TDuration")) ? null : Long.valueOf(clientDataMap.get("TDuration")));

        clientData.setAgent(agentCLI);
        clientData.setRemoteIP(getString(genericRecord.get("remoteIP")));
        clientData.setRlogid(getString(genericRecord.get("rlogid")));
        clientData.setUrlQueryString(getString(applicationPayload.get("urlQueryString")));

        UbiEvent ubiEvent = new UbiEvent();
        ubiEvent.setGuid(getString(genericRecord.get("guid")));

        ubiEvent.setAppId(getInteger(genericRecord.get("appId")));
        ubiEvent.setClientData(clientData);
        ubiEvent.setUrlQueryString(clientData.getUrlQueryString());
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(applicationPayload));
        ubiEvent.setPageName(clientData.getTName());
        ubiEvent.setCurrentImprId(getLong(applicationPayload.get("currentImprId")));
        ubiEvent.setAgentInfo(clientData.getAgent());
        ubiEvent.setClickId(getInteger(genericRecord.get("clickId")));
        ubiEvent.setClientIP(getString(genericRecord.get("clientIP")));
        ubiEvent.setCobrand(getInteger(genericRecord.get("cobrand")));
        ubiEvent.setCookies(getString(genericRecord.get("cookies")));
        ubiEvent.setFlags(getString(genericRecord.get("flags")));
        ubiEvent.setIframe(getBoolean(genericRecord.get("flags")));
        ubiEvent.setItemId(IntegerField.getIntVal(getString(genericRecord.get("itemId"))));
        ubiEvent.setPageId(getInteger(genericRecord.get("pageId")));
        ubiEvent.setRdt(getInteger(genericRecord.get("rdt")) != 0);
        ubiEvent.setRefererHash(getLong(genericRecord.get("refererHash")));
        ubiEvent.setReferrer(getString(applicationPayload.get("Referer")));
        ubiEvent.setRegu(getInteger(getString(applicationPayload.get("regu"))));
        ubiEvent.setWebServer(getString(applicationPayload.get("webServer")));
        ubiEvent.setSourceImprId(getLong(getString(applicationPayload.get("sourceImprId"))));
        ubiEvent.setSiteId(getInteger(genericRecord.get("siteId")));
        ubiEvent.setSqr(getString(genericRecord.get("sqr")));
        ubiEvent.setEventTimestamp(getLong(getString(genericRecord.get("eventTimestamp"))));
        ubiEvent.setSojDataDt(SOJTS2Date.castSojTimestampToDate(getLong(getString(genericRecord.get("eventTimestamp")))));
        ubiEvent.setOldSessionSkey(null);
        ubiEvent.setUserId(getString(genericRecord.get("userId")));
        Integer pageId = ubiEvent.getPageId();
        Map<Integer, Integer> findingFlagMap = LkpFetcher.getInstance().getFindingFlagMap();
        if (findingFlagMap.containsKey(pageId)) {
            ubiEvent.setBitVal(findingFlagMap.get(pageId));
        }

        ubiEvent.setPartialValidPage(true);//set to default value;
        ubiEvent.setStaticPageType(-1);

        return ubiEvent;

    }

    private Integer getInteger( Object o ) {
        if (StringUtils.isEmpty(getString(o))) {
            return null;
        } else {
            return Integer.valueOf(getString(o));
        }
    }

    private boolean getBoolean( Object o ) {
        if (StringUtils.isEmpty(getString(o))) {
            return false;
        } else {
            return Boolean.valueOf(getString(o));
        }
    }


    private Long getLong( Object o ) {
        if (StringUtils.isEmpty(getString(o))) {
            return null;
        } else {
            return Long.valueOf(getString(o));
        }
    }

    private String getString( Object o ) {
        return (o != null && !"null".equals(o.toString())) ? o.toString() : null;
    }

    private String getString2( Object o ) {
        return (o != null) ? o.toString() : "";
    }
}