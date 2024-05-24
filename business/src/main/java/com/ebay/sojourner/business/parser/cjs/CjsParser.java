package com.ebay.sojourner.business.parser.cjs;

import com.ebay.sojourner.business.parser.FieldParser;
import com.ebay.sojourner.business.parser.JSColumnParser;
import com.ebay.sojourner.business.parser.ParserContext;
import com.ebay.sojourner.cjs.util.CjsFormulaInterpreter;
import com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext;
import com.ebay.sojourner.cjs.util.SignalContext;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import static com.ebay.sojourner.cjs.service.CjsMetadataManager.getCjsBetaMetadataProvider;
import static com.ebay.sojourner.cjs.service.CjsMetadataManager.getCjsMetadataProvider;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_METRIC_PREFIX;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJSBETA_TAG_NAME;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_METRIC_PREFIX;
import static com.ebay.sojourner.cjs.util.CjsFormulaInterpreterContext.CJS_TAG_NAME;
import static com.ebay.sojourner.cjs.util.ConditionalMap.mapIfNotNull;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_CJSBETA_DISABLED;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_CJS_DISABLED;
import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_PARSER_FILTER_ENABLED;

/**
 * Invoke {@link UbiEvent#setApplicationPayload(String)}.
 * Relies on {@link JSColumnParser} for device detection.
 */
@Slf4j
public class CjsParser implements FieldParser<RawEvent, UbiEvent> {

    public static final String CJS_GEN_TAG = "cjs.cjs.generate_tag";
    public static final String CJSBETA_GEN_TAG = "cjs.cjsBeta.generate_tag";

    public static final String CJS_PARSER_FILTER_REJECT = "cjs.parser.filter.reject";
    public static final String CJS_PARSER_FILTER_ACCEPT = "cjs.parser.filter.accept";

    public static final String CJS_PROCESS_NS = "cjs.cjs.process.ns";
    public static final String CJSBETA_PROCESS_NS = "cjs.cjsBeta.process.ns";

    private static final Set<String> E_FAM_PRE_FILTER_SET = ImmutableSet.of("ITM", "LST", "XOUT");

    private CjsFormulaInterpreter cjsFormulaInterpreter;
    private CjsFormulaInterpreter cjsBetaFormulaInterpreter;
    private ParserContext context;

    private BiPredicate<RawEvent, UbiEvent> preFilterFunc = (x, y) -> true;

    @Override
    public void init(ParserContext context) throws Exception {
        this.context = context;

        if (context.get(CJS_PARSER_FILTER_ENABLED, false)) {
            preFilterFunc = this::preFilter;
            context.invokeTrigger(CJS_PARSER_FILTER_ENABLED, null);
        }

        if (!context.get(CJS_CJS_DISABLED, false)) {
            cjsFormulaInterpreter = new CjsFormulaInterpreter(
                    new CjsFormulaInterpreterContext(
                            CJS_TAG_NAME,
                            getCjsMetadataProvider(),
                            CJS_METRIC_PREFIX,
                            context::invokeTrigger
                    )
            );
        } else {
            context.invokeTrigger(CJS_CJS_DISABLED, null);
        }

        if (!context.get(CJS_CJSBETA_DISABLED, false)) {
            cjsBetaFormulaInterpreter = new CjsFormulaInterpreter(
                    new CjsFormulaInterpreterContext(
                            CJSBETA_TAG_NAME,
                            getCjsBetaMetadataProvider(),
                            CJSBETA_METRIC_PREFIX,
                            context::invokeTrigger
                    )
            );
        } else {
            context.invokeTrigger(CJS_CJSBETA_DISABLED, null);
        }
    }

    @Override
    public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {

        if (preFilterFunc.test(rawEvent, ubiEvent)) {
            context.invokeTrigger(CJS_PARSER_FILTER_ACCEPT, null);
        } else {
            context.invokeTrigger(CJS_PARSER_FILTER_REJECT, null);
            return;
        }

        SignalContext cjsContext = SignalContext.getThreadLocalContext(rawEvent, ubiEvent);

        val stopwatch = Stopwatch.createUnstarted();
        val cjs = mapIfNotNull(cjsFormulaInterpreter, x -> {
            stopwatch.reset().start();
            val result = x.interpret(cjsContext);
            context.invokeTrigger(CJS_PROCESS_NS, stopwatch.stop().elapsed().toNanos());
            return result;
        });

        val cjsBeta = mapIfNotNull(cjsBetaFormulaInterpreter, x -> {
            stopwatch.reset().start();
            val result = x.interpret(cjsContext);
            context.invokeTrigger(CJSBETA_PROCESS_NS, stopwatch.stop().elapsed().toNanos());
            return result;
        });

        Map<String, String> map = new HashMap<>();
        if (StringUtils.isNotBlank(cjs)) {
            map.put(CJS_TAG_NAME, cjs);
            context.invokeTrigger(CJS_GEN_TAG, cjs);
        }
        if (StringUtils.isNotBlank(cjsBeta)) {
            map.put(CJSBETA_TAG_NAME, cjsBeta);
            context.invokeTrigger(CJSBETA_GEN_TAG, cjsBeta);
        }

        if (!map.isEmpty()) {
            val cjsString = PropertyUtils.mapToString(map);
            val applicationPayload = ubiEvent.getApplicationPayload();

            if (StringUtils.isNotBlank(cjsString)) {
                if (StringUtils.isNotBlank(applicationPayload)) {
                    ubiEvent.setApplicationPayload(applicationPayload + "&" + cjsString);
                } else {
                    ubiEvent.setApplicationPayload(cjsString);
                }
                // log.info("CJS Injected: {}", ubiEvent);
            }
        }
    }

    private boolean preFilter(RawEvent rawEvent, UbiEvent ubiEvent) {
        return (
                Objects.nonNull(ubiEvent.getEventFamily()) &&
                        E_FAM_PRE_FILTER_SET.contains(ubiEvent.getEventFamily())
        ) || (
                // TODO: currently all ROI events in SOJ have itm tag, but better to have a separate event family
                Objects.nonNull(ubiEvent.getItemId()) && ubiEvent.getItemId() > 0
        );
    }
}
