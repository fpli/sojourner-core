package com.ebay.sojourner.rt.operator.event;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.ebay.sojourner.business.detector.NewEventBotDetector;
import com.ebay.sojourner.business.parser.EventParser;
import com.ebay.sojourner.business.parser.ParserContext;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJSBETA_GEN_TAG;
import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJSBETA_PROCESS_NS;
import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJS_GEN_TAG;
import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJS_PARSER_FILTER_ACCEPT;
import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJS_PARSER_FILTER_REJECT;
import static com.ebay.sojourner.business.parser.cjs.CjsParser.CJS_PROCESS_NS;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_DEVICE_CTX_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_INVOCATION_FAILURE;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_JEXL_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_MATCH_FILTER_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_NULL_POINTER_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_DEVICE_CTX_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_INVOCATION_FAILURE;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_JEXL_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_MATCH_FILTER_METRIC;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_NULL_POINTER_EXCEPTION_METRIC;
import static com.ebay.sojourner.cjs.util.ConditionalInvoke.invokeIf;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_CJSBETA_DISABLED;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_CJS_DISABLED;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_PARSER_DISABLED;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_PARSER_FILTER_ENABLED;
import static com.ebay.sojourner.common.util.Constants.SOJ_METRIC_GROUP;

@Slf4j
public class EventMapFunction extends RichMapFunction<RawEvent, UbiEvent> {

    private EventParser parser;
    private AverageAccumulator avgEventParserDuration;
    private NewEventBotDetector newEventBotDetector;
    private Map<String, Object> cjsConfigMap;

    public EventMapFunction(Map<String, Object> cjsConfigMap) {
        this.cjsConfigMap = cjsConfigMap;
    }

    @Override
    public void open(Configuration conf) throws Exception {
        super.open(conf);

        val contextBuilder = initCjsFactoriesAndTriggers(new ParserContext.Builder(),
                                                         getRuntimeContext().getMetricGroup(),
                                                         cjsConfigMap);

        parser = new EventParser(contextBuilder.build());
        avgEventParserDuration = new AverageAccumulator();
        newEventBotDetector = new NewEventBotDetector();
        getRuntimeContext()
                .addAccumulator("Average Duration of Event Parsing", avgEventParserDuration);
    }

    @Override
    public UbiEvent map(RawEvent rawEvent) throws Exception {
        UbiEvent event = new UbiEvent();
        long startTimeForEventParser = System.nanoTime();
        parser.parse(rawEvent, event);
        avgEventParserDuration.add(System.nanoTime() - startTimeForEventParser);
        Set<Integer> eventBotFlagList = newEventBotDetector.getBotFlagList(event);
        event.getBotFlags().addAll(eventBotFlagList);
        return event;
    }

    private static ParserContext.Builder initCjsFactoriesAndTriggers(
            ParserContext.Builder context, OperatorMetricGroup metricGroup,
            Map<String, Object> cjsConfigMap) {

        if (Objects.nonNull(cjsConfigMap) && !cjsConfigMap.isEmpty()) {
            cjsConfigMap.entrySet().forEach(
                    entry -> context.registerConfig(entry.getKey(), entry::getValue)
            );
        }

        if (Objects.nonNull(metricGroup)) {
            val cjsParserDisabledCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_PARSER_DISABLED);
            val cjsParserFilterEnabledCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_PARSER_FILTER_ENABLED);
            val cjsDisabledCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_CJS_DISABLED);
            val cjsBetaDisabledCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_CJSBETA_DISABLED);

            context.registerTrigger(CJS_PARSER_DISABLED, param -> cjsParserDisabledCounter.inc())
                   .registerTrigger(CJS_PARSER_FILTER_ENABLED, param -> cjsParserFilterEnabledCounter.inc())
                   .registerTrigger(CJS_CJS_DISABLED, param -> cjsDisabledCounter.inc())
                   .registerTrigger(CJS_CJSBETA_DISABLED, param -> cjsBetaDisabledCounter.inc());

            val cjsParserFilterAcceptCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_PARSER_FILTER_ACCEPT);
            val cjsParserFilterRejectCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_PARSER_FILTER_REJECT);

            context.registerTrigger(CJS_PARSER_FILTER_ACCEPT, param -> cjsParserFilterAcceptCounter.inc())
                   .registerTrigger(CJS_PARSER_FILTER_REJECT, param -> cjsParserFilterRejectCounter.inc());

            val cjsJexlExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_JEXL_EXCEPTION_METRIC);
            val cjsDeviceCtxExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_DEVICE_CTX_EXCEPTION_METRIC);
            val cjsNullPointerExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_NULL_POINTER_EXCEPTION_METRIC);
            val cjsMatchFilterCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_MATCH_FILTER_METRIC);
            val cjsGenTagCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_GEN_TAG);
            val cjsInvocationFailureCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJS_INVOCATION_FAILURE);

            val cjsGenTagHistogram = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).histogram(CJS_PROCESS_NS, new DropwizardHistogramWrapper(
                            new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES))
                    ));

            context.registerTrigger(CJS_JEXL_EXCEPTION_METRIC, param -> cjsJexlExceptionCounter.inc())
                   .registerTrigger(CJS_DEVICE_CTX_EXCEPTION_METRIC, param -> cjsDeviceCtxExceptionCounter.inc())
                   .registerTrigger(CJS_NULL_POINTER_EXCEPTION_METRIC, param -> cjsNullPointerExceptionCounter.inc())
                   .registerTrigger(CJS_MATCH_FILTER_METRIC, param -> cjsMatchFilterCounter.inc())
                   .registerTrigger(CJS_GEN_TAG, param -> cjsGenTagCounter.inc())
                   .registerTrigger(CJS_PROCESS_NS, param ->
                           invokeIf(param, x -> Objects.nonNull(x) && x instanceof Long,
                                    x -> cjsGenTagHistogram.update((long) x))
                   )
                   .registerTrigger(CJS_INVOCATION_FAILURE, param -> cjsInvocationFailureCounter.inc());

            val cjsBetaJexlExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_JEXL_EXCEPTION_METRIC);
            val cjsBetaDeviceCtxExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_DEVICE_CTX_EXCEPTION_METRIC);
            val cjsBetaNullPointerExceptionCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_NULL_POINTER_EXCEPTION_METRIC);
            val cjsBetaMatchFilterCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_MATCH_FILTER_METRIC);
            val cjsBetaGenTagCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_GEN_TAG);
            val cjsBetaInvocationFailureCounter = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).counter(CJSBETA_INVOCATION_FAILURE);

            val cjsBetaGenTagHistogram = metricGroup
                    .addGroup(SOJ_METRIC_GROUP).histogram(CJSBETA_PROCESS_NS, new DropwizardHistogramWrapper(
                            new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES))
                    ));

            context.registerTrigger(CJSBETA_JEXL_EXCEPTION_METRIC, param -> cjsBetaJexlExceptionCounter.inc())
                   .registerTrigger(CJSBETA_DEVICE_CTX_EXCEPTION_METRIC,
                                    param -> cjsBetaDeviceCtxExceptionCounter.inc())
                   .registerTrigger(CJSBETA_NULL_POINTER_EXCEPTION_METRIC,
                                    param -> cjsBetaNullPointerExceptionCounter.inc())
                   .registerTrigger(CJSBETA_MATCH_FILTER_METRIC, param -> cjsBetaMatchFilterCounter.inc())
                   .registerTrigger(CJSBETA_GEN_TAG, param -> cjsBetaGenTagCounter.inc())
                   .registerTrigger(CJSBETA_PROCESS_NS, param ->
                           invokeIf(param, x -> Objects.nonNull(x) && x instanceof Long,
                                    x -> cjsBetaGenTagHistogram.update((long) x))
                   )
                   .registerTrigger(CJSBETA_INVOCATION_FAILURE, param -> cjsBetaInvocationFailureCounter.inc());
        }

        return context;
    }
}
