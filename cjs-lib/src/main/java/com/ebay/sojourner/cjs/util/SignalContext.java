package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.cjs.service.JexlService;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.tracking.schema.cjs.CJSignal;
import com.ebay.tracking.schema.cjs.DeviceContext;
import com.ebay.tracking.schema.cjs.SignalKind;
import com.ebay.tracking.schema.cjs.SignalType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import org.apache.commons.jexl3.JexlContext;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Getter
@Setter(AccessLevel.PRIVATE)
public final class SignalContext {

    private static final LoadingCache<String, DeviceContext> DEVICE_CONTEXT_CACHE =
            CacheBuilder.newBuilder()
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build(new CacheLoader<String, DeviceContext>() {
                            @NotNull
                            @Override
                            public DeviceContext load(String experience) {
                                return DeviceContext.apply(experience);
                            }
                        });

    private static final ThreadLocal<SignalContext> SIG_CONTEXT = ThreadLocal.withInitial(SignalContext::new);

    private static final ThreadLocal<CJSignal> CJS = ThreadLocal.withInitial(CJSignal::new);

    private RawEvent rawEvent;

    private UbiEvent ubiEvent;

    private static final DeviceExperienceDetectionStrategy deviceExperienceDetectionStrategy =
            new SimpleDeviceExperienceDetectionStrategy();

    private SignalContext() {
    }

    public SignalContext(@NonNull RawEvent rawEvent, @NonNull UbiEvent ubiEvent) {
        this.rawEvent = rawEvent;
        this.ubiEvent = ubiEvent;
    }

    public JexlContext getJexlContext() {
        val context = JexlService.getContext();
        context.clear();
        context.set("rawEvent", getRawEvent());
        context.set("event", getUbiEvent());

        return context;
    }

    public String getDeviceContextExperience() {
        return deviceExperienceDetectionStrategy.detectDevice(this);
    }

    public static SignalContext getThreadLocalContext(@NonNull RawEvent rawEvent, @NonNull UbiEvent ubiEvent) {
        val context = SIG_CONTEXT.get();
        context.setRawEvent(rawEvent);
        context.setUbiEvent(ubiEvent);
        return context;
    }

    public static CJSignal getThreadLocalCJSignal(@NonNull String domain,
                                                  @NonNull String signalType,
                                                  @NonNull String signalName,
                                                  @NonNull String signalId,
                                                  @NonNull String experience) throws ExecutionException {

        val cjSignal = CJS.get();
        cjSignal.kind_$eq(SignalKind.apply(domain, SignalType.valueOf(signalType), signalName));
        cjSignal.signalId_$eq(signalId);
        cjSignal.deviceContext_$eq(DEVICE_CONTEXT_CACHE.get(experience));

        return cjSignal;
    }
}
