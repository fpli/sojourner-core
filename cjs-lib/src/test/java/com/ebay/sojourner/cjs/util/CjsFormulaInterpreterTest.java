package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.ebay.tracking.Decoders;
import com.ebay.tracking.schema.cjs.CJSignal;
import com.ebay.tracking.schema.cjs.DeviceContext;
import com.ebay.tracking.schema.cjs.SignalKind;
import com.ebay.tracking.schema.cjs.SignalType;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static com.ebay.sojourner.cjs.service.CjsMetadataManager.getCjsBetaMetadataProvider;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreter.interpretInternal;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSMOCK_METRIC_PREFIX;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSMOCK_TAG_NAME;
import static com.ebay.sojourner.cjs.util.ConditionalMap.mapIfNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CjsFormulaInterpreterTest {

    private final MessageEncoder<CJSignal> cjSignalMessageEncoder =
            new BinaryMessageEncoder<>(new SpecificData(), CJSignal.SCHEMA$());

    private final CjsFormulaInterpreterContext context = new CjsFormulaInterpreterContext(
            CJSMOCK_TAG_NAME,
            getCjsBetaMetadataProvider(),
            CJSMOCK_METRIC_PREFIX,
            (x, y) -> {
                // Do nothing
            }
    );


    @Test
    void testNewInstanceNotNull() {
        val cjsFormulaInterpreter = new CjsFormulaInterpreter(context);

        assertNotNull(cjsFormulaInterpreter);
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForSrpSucceed() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("srpGist", "base64");
        sojA.put("p", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setPageId(123456);
        ubiEvent.setCurrentImprId(123456L);
        ubiEvent.setUserId("uid_123456");
        ubiEvent.setItemId(123456L);
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Search", SignalType.IMPRESSION, "SRP"),
                UUIDUtils.fromAny(ubiEvent.getCurrentImprId()),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForSrpSucceed2() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("srpGist", "base64");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setPageId(4465145);
        ubiEvent.setCurrentImprId(123456L);
        ubiEvent.setUserId("uid_123456");
        ubiEvent.setItemId(123456L);
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Search", SignalType.IMPRESSION, "SRP"),
                UUIDUtils.fromAny(ubiEvent.getCurrentImprId()),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }


    @SneakyThrows
    @Test
    void testInvokeInterpretForSrpFail() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("srpGist", "base64");
        sojA.put("p", "4465146");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setPageId(4465146);
        ubiEvent.setCurrentImprId(123456L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        assertNull(interpretInternal(context, signalContext));
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForSrpFail2() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("CJS_ABC", "CJS_ABC");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setPageId(4465146);
        ubiEvent.setCurrentImprId(123456L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        assertNull(interpretInternal(context, signalContext));
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForViewItemSucceed() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setRdt(false);
        ubiEvent.setItemId(123456L);
        ubiEvent.setPageId(2349624);
        ubiEvent.setCurrentImprId(123456L);
        ubiEvent.setEventAction("EXPC");

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("ViewItem", SignalType.IMPRESSION, "VI"),
                UUIDUtils.fromAny(ubiEvent.getCurrentImprId()),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForWatchSucceed() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setItemId(123456L);
        ubiEvent.setPageId(2055415);
        ubiEvent.setCurrentImprId(123456L);

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Watch", SignalType.OUTCOME, "ItemWatched"),
                UUIDUtils.fromAny(rawEvent.getSojA().get("eventPrimaryId")),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }


    @SneakyThrows
    @Test
    void testUUIDGeneratorForWatchItemWithValue() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setItemId(123456L);
        ubiEvent.setPageId(2055415);
        ubiEvent.setCurrentImprId(123456L);

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Watch", SignalType.OUTCOME, "ItemWatched"),
                UUIDUtils.fromAny(rawEvent.getSojA().get("eventPrimaryId")),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testUUIDGeneratorForWatchItemWithEmptyValue() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setItemId(123456L);
        ubiEvent.setPageId(2055415);
        ubiEvent.setCurrentImprId(123456L);

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);
        val signal = CJSignal.apply(
                SignalKind.apply("Watch", SignalType.OUTCOME, "ItemWatched"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                             x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInstanceMethod() {

        val intepreter = new CjsFormulaInterpreter(context);

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setAgentInfo("ebayUserAgent/eBayIOS;6.141.0;iOS;16.2;Apple;iPhone13_2;--;390x844;3.0");
        ubiEvent.setItemId(123456L);
        ubiEvent.setPageId(2055415);
        ubiEvent.setCurrentImprId(123456L);

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);
        val signal = CJSignal.apply(
                SignalKind.apply("Watch", SignalType.OUTCOME, "ItemWatched"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(intepreter.interpret(signalContext)))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForAdd2Cart() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("cartopstate", "non-empty");
        sojA.put("cartaction", "ADD_TO_CART");
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setPageId(2364840);
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Cart", SignalType.OUTCOME, "Add2Cart"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                        x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForBid() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("saleTypeFlow", "BID");
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setPageId(2483445);
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Bid", SignalType.OUTCOME, "Bid"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                        x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForBIN() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("saleTypeFlow", "BIN");
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setPageId(2483445);
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("BIN", SignalType.OUTCOME, "BIN"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                        x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForViewport() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("viewport", "non-empty");
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setEventFamily("LST");
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Search", SignalType.IMPRESSION, "SRP_Viewport"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                        x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }

    @SneakyThrows
    @Test
    void testInvokeInterpretForInteraction() {

        val rawEvent = new RawEvent();
        Map<String, String> sojA = Maps.newHashMap();
        sojA.put("interaction", "non-empty");
        sojA.put("eventPrimaryId", "123456");
        rawEvent.setSojA(sojA);

        val ubiEvent = new UbiEvent();
        ubiEvent.setEventFamily("LST");
        ubiEvent.setEventAction("ACTN");
        ubiEvent.setEventTimestamp(1640000000L);
        ubiEvent.setApplicationPayload(PropertyUtils.mapToString(sojA));

        val signalContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val signal = CJSignal.apply(
                SignalKind.apply("Search", SignalType.ACTION, "SRP_Interaction"),
                Optional.ofNullable(UUIDUtils.fromAny(Optional.ofNullable(rawEvent.getSojA()).map(s -> s.get("eventPrimaryId")).orElse(null))).orElse(""),
                DeviceContext.apply(signalContext.getDeviceContextExperience()));

        assertEquals(
                signal,
                Decoders.cjs().decode(Base64.getDecoder().decode(interpretInternal(context, signalContext)))
        );

        assertEquals(
                interpretInternal(context, signalContext),
                mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                        x -> Base64.getEncoder().encodeToString(x.array()))
        );
    }
}