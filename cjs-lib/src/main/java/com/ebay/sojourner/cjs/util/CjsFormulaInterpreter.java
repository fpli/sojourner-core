package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.cjs.service.JexlService;
import com.ebay.tracking.schema.cjs.CJSignal;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.DEVICE_CTX_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.INVOCATION_FAILURE;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.JEXL_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.MATCH_FILTER_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.NULL_POINTER_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.ConditionalMap.mapIf;
import static com.ebay.sojourner.cjs.util.ConditionalMap.mapIfNotNull;

@Slf4j
public class CjsFormulaInterpreter {

    private static final MessageEncoder<CJSignal> cjSignalMessageEncoder =
            new BinaryMessageEncoder<>(new SpecificData(), CJSignal.SCHEMA$());

    @NonNull
    private final CjsFormulaInterpreterContext context;

    public CjsFormulaInterpreter(@NonNull CjsFormulaInterpreterContext context) {
        this.context = context;
    }

    public String interpret(SignalContext signalContext) {
        try {
            return interpretInternal(context, signalContext);
        } catch (JexlException e) {
            log.error("Failed to interpret signal: {}", signalContext.getUbiEvent(), e);
            context.invokeTriggerPrefixed(JEXL_EXCEPTION_METRIC, e);
        } catch (NullPointerException e) {
            log.error("Failed to interpret signal: {}", signalContext.getUbiEvent(), e);
            context.invokeTriggerPrefixed(NULL_POINTER_EXCEPTION_METRIC, e);
        } catch (Exception e) {
            log.error("Failed to interpret signal: {}", signalContext.getUbiEvent(), e);
            context.invokeTriggerPrefixed(INVOCATION_FAILURE, null);
        }

        return null;
    }

    @SneakyThrows
    public static String interpretInternal(CjsFormulaInterpreterContext context, SignalContext signalContext) {

        val definitions = context.getMetadataProvider().get();

        val firstMatch = definitions
                .values().stream()
                .flatMap(value -> value
                        .getLogicalDefinition().stream()
                        .flatMap(logicDefinition -> logicDefinition
                                .getEventClassifiers().stream()
                                .map(ec -> mapIf(ec, x -> x.match(signalContext), x -> Pair.of(value, x)))
                        )
                )
                .filter(Objects::nonNull)
                .findFirst();

        if (firstMatch.isPresent()) {
            context.invokeTriggerPrefixed(MATCH_FILTER_METRIC, null);

            val definition = firstMatch.get().getLeft();
            val classifier = firstMatch.get().getRight();
            val signalId = definition
                    .getLogicalDefinition().stream()
                    .map(logicDefinition ->
                                 mapIfNotNull(
                                         JexlService.getExpression(logicDefinition.getUuidGenerator().getFormula()),
                                         expression -> (String) expression.evaluate(signalContext.getJexlContext())
                                 )
                    )
                    .filter(Objects::nonNull)
                    .findFirst().orElse(StringUtils.EMPTY);
            val domain = StringUtils.defaultString(definition.getDomain());
            val type = StringUtils.defaultString(classifier.getType());
            val name = StringUtils.defaultString(classifier.getName());

            String deviceContextExperience;
            try {
                deviceContextExperience = signalContext.getDeviceContextExperience();
            } catch (Exception e) {
                log.debug("Failed to generate {}.deviceContext.experience from UbiEvent: {}",
                          context.getTagName(), signalContext.getUbiEvent(), e);

                context.invokeTriggerPrefixed(DEVICE_CTX_EXCEPTION_METRIC, e);
                throw e;
            }

            val signal = SignalContext.getThreadLocalCJSignal(domain, type, name, signalId, deviceContextExperience);

            return mapIfNotNull(cjSignalMessageEncoder.encode(signal), ByteBuffer::hasArray,
                                x -> Base64.getEncoder().encodeToString(x.array()));
        }

        return null;
    }

}
