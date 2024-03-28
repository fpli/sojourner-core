package com.ebay.sojourner.cjs.service;

import com.ebay.sojourner.cjs.util.UUIDUtils;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * <pre>
 * Effect of SAFE option:
 * - When SAFE is true: <undefined> will throw an exception; <undefined>.foo will return null.
 * - When SAFE is false(RECOMMENDED): <undefined> & <undefined>.foo will throw an exception.
 * </pre>
 */
@Slf4j
class JexlServiceTest {

    @Test
    void invokeWithEmptyContext() {
        Map<String, Object> funcs = ImmutableMap.of(
                "uuid", UUIDUtils.class,
                "console", System.out
        );

        val restrictedEngine = new JexlBuilder()
                .permissions(JexlPermissions.UNRESTRICTED)
                .namespaces(funcs)
                .safe(false) // It is recommended to use safe(false) as an explicit default.
                .create();

        val unrestrictedEngine = new JexlBuilder()
                .permissions(JexlPermissions.UNRESTRICTED)
                .namespaces(funcs)
                .create();

        JexlContext context = JexlEngine.EMPTY_CONTEXT;

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo").evaluate(context));

        assertNull(unrestrictedEngine.createExpression("foo.bar").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar").evaluate(context));

        assertNull(unrestrictedEngine.createExpression("foo?.bar").evaluate(context));
        assertNull(restrictedEngine.createExpression("foo?.bar").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar + 1").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar + 1").evaluate(context));

        assertFalse((Boolean) unrestrictedEngine.createExpression("foo.bar == ''").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar == ''").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar = 1").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar = 1").evaluate(context));
    }

    @Test
    void invokeWithNonEmptyContext() {
        Map<String, Object> funcs = ImmutableMap.of(
                "uuid", UUIDUtils.class,
                "console", System.out
        );

        val context = new MapContext(ImmutableMap.of(
                "foo", new Object()
        ));

        val restrictedEngine = new JexlBuilder()
                .permissions(JexlPermissions.UNRESTRICTED)
                .namespaces(funcs)
                .safe(false) // It is recommended to use safe(false) as an explicit default.
                .create();

        val unrestrictedEngine = new JexlBuilder()
                .permissions(JexlPermissions.UNRESTRICTED)
                .namespaces(funcs)
                .create();

        assertNotNull(unrestrictedEngine.createExpression("foo").evaluate(context));
        assertNotNull(restrictedEngine.createExpression("foo").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar").evaluate(context));

        assertNull(unrestrictedEngine.createExpression("foo?.bar").evaluate(context));
        assertNull(restrictedEngine.createExpression("foo?.bar").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar + 1").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar + 1").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar == ''").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar == ''").evaluate(context));

        assertThrows(JexlException.class,
                     () -> unrestrictedEngine.createExpression("foo.bar = 1").evaluate(context));
        assertThrows(JexlException.class,
                     () -> restrictedEngine.createExpression("foo.bar = 1").evaluate(context));
    }
}
