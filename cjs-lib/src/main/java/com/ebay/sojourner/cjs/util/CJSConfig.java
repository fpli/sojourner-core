package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CJSConfig {

    private static final Map<String, Object> confData = new ConcurrentHashMap<>();
    private static final String DEFAULT_FILE = "cjs.properties";
    private static Properties cjsProperties;

    static {
        InputStream resource = CJSConfig.class.getClassLoader().getResourceAsStream(DEFAULT_FILE);
        cjsProperties = initProperties(resource);
        try {
            initConfiguration(false);
        } catch (Exception e) {
            log.error("Cannot init Configurations", e);
        }
    }

    private CJSConfig() {
        // private
    }

    private static Properties initProperties(InputStream filePath) {
        try {
            return PropertyUtils.loadInProperties(filePath);
        } catch (Exception e) {
            log.error("Unable to load resource", e);
            throw new RuntimeException(e);
        }
    }

    private static void setBoolean(String key, Boolean value) {
        Preconditions.checkNotNull(key, "Key must not be null.");
        Preconditions.checkNotNull(value, "Value must not be null.");

        synchronized (CJSConfig.class) {
            confData.put(key, value);
        }
    }

    protected static void setString(String key, String value) {
        Preconditions.checkNotNull(key, "Key must not be null.");
        Preconditions.checkNotNull(value, "Value must not be null.");

        synchronized (CJSConfig.class) {
            confData.put(key, value);
        }
    }

    public static String getString(String key) {
        Object o = getRawValue(key);
        if (o == null) {
            return null;
        } else {
            return o.toString();
        }
    }

    private static Object getRawValue(String key) {
        Preconditions.checkNotNull(key, "Key must not be null.");
        return confData.get(key);
    }

    private static void initConfiguration(boolean enableTest) throws Exception {

        if (getCJSProperty(Property.LOG_LEVEL) != null) {
            setString(Property.LOG_LEVEL, getCJSProperty(Property.LOG_LEVEL));
        }

        if (enableTest) {
            setBoolean(Property.IS_TEST_ENABLE, true);
        } else {
            setString(Property.CJSBETA_METADATA_JSON, getCJSProperty(Property.CJSBETA_METADATA_JSON));
            setString(Property.CJS_METADATA_JSON, getCJSProperty(Property.CJS_METADATA_JSON));
            setString(Property.CJS_LKP_PATH, getCJSProperty(Property.CJS_LKP_PATH));
        }
    }

    public static String getCJSProperty(String property) {
        return cjsProperties.getProperty(property);
    }
}
