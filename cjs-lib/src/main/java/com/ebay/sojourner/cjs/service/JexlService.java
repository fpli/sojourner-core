package com.ebay.sojourner.cjs.service;

import com.ebay.sojourner.cjs.util.UUIDUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.Map;

@Slf4j
public class JexlService {

    private static final LoadingCache<String, JexlExpression> EXPRESSION_CACHE =
            CacheBuilder.newBuilder()
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build(new JexlExpressionLoader());

    private static final ThreadLocal<MapContext> MAP_CONTEXT = ThreadLocal.withInitial(MapContext::new);

    private static class JexlExpressionLoader extends CacheLoader<String, JexlExpression> {

        private final JexlEngine engine;

        JexlExpressionLoader() {
            Map<String, Object> funcs = ImmutableMap.of(
                    "uuid", UUIDUtils.class
            );

            engine = new JexlBuilder()
                    .permissions(JexlPermissions.UNRESTRICTED)
                    .namespaces(funcs)
                    .safe(false) // It is recommended to use safe(false) as an explicit default.
                    .create();
        }

        @Override
        public JexlExpression load(@NotNull String key) throws Exception {
            return engine.createExpression(key);
        }
    }

    public static JexlExpression getExpression(String expression) {
        try {
            if (StringUtils.isBlank(expression)) {
                return null;
            }
            return EXPRESSION_CACHE.get(expression);
        } catch (Exception ex) {
            log.error("failed to get expression for formula: {}", expression, ex);
            return null;
        }
    }

    public static MapContext getContext() {
        return MAP_CONTEXT.get();
    }

}
