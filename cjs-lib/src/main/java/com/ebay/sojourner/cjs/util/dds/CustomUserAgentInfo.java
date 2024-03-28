package com.ebay.sojourner.cjs.util.dds;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.ebay.sojourner.cjs.util.ConditionalInvoke.invokeIfNotBlankStr;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.ANDROID;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.CUSTOM_UA_FIELD_DELIMITER;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.IOS;

@Slf4j
@Getter
@Setter(AccessLevel.PRIVATE)
@EqualsAndHashCode
public class CustomUserAgentInfo {

    private static final ThreadLocal<CustomUserAgentInfo> CUSTOM_USER_AGENT_INFO =
            ThreadLocal.withInitial(CustomUserAgentInfo::new);

    public static final String APP_NAME = "app-name";
    public static final String APP_VERSION = "app-version";
    public static final String OS_NAME = "os-name";
    public static final String OS_VERSION = "os-version";
    public static final String MFG = "device-mfg";
    public static final String MODEL = "device-model";
    public static final String CARRIER = "carrier";
    public static final String VIEWPORT = "display-resolution";
    public static final String DPI = "display-density";

    public static final Map<String, Integer> FIELD_MAP;

    static {
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put(APP_NAME, 0);
        fieldMap.put(APP_VERSION, 1);
        fieldMap.put(OS_NAME, 2);
        fieldMap.put(OS_VERSION, 3);
        fieldMap.put(MFG, 4);
        fieldMap.put(MODEL, 5);
        fieldMap.put(CARRIER, 6);
        fieldMap.put(VIEWPORT, 7);
        fieldMap.put(DPI, 8);

        FIELD_MAP = Collections.unmodifiableMap(fieldMap);
    }

    private String appName;

    private String appVersion;

    private String osName;

    private String osVersion;

    private String manufacturer;

    private String model;

    private CustomUserAgentInfo() {
    }

    public CustomUserAgentInfo(String customUserAgent) {
        refresh(customUserAgent);
    }

    private void refresh(String customUserAgent) {
        if (StringUtils.isNotBlank(customUserAgent)) {
            String[] fields = customUserAgent.split(CUSTOM_UA_FIELD_DELIMITER, -1);
            if (fields.length > 0) {
                extractOsNameNAppName(fields);
                invokeIfNotBlankStr(getFieldInfoByKey(APP_VERSION, fields), x -> this.appVersion = x);
                invokeIfNotBlankStr(getFieldInfoByKey(OS_VERSION, fields), x -> this.osVersion = x);
                invokeIfNotBlankStr(getFieldInfoByKey(MFG, fields), x -> this.manufacturer = x);
                invokeIfNotBlankStr(getFieldInfoByKey(MODEL, fields), x -> this.model = x);
            }
        }
    }

    private void extractOsNameNAppName(String[] rawFieldList) {
        this.osName = getFieldInfoByKey(OS_NAME, rawFieldList);

        invokeIfNotBlankStr(
                getFieldInfoByKey(APP_NAME, rawFieldList),
                x -> this.appName = x.substring(x.indexOf("/") + 1)
        );

        if (StringUtils.isEmpty(osName) && !StringUtils.isEmpty(appName)) {
            if (appName.equalsIgnoreCase("eBayIOS")) {
                this.osName = IOS;
            } else if (appName.equalsIgnoreCase("eBayAndroid")) {
                this.osName = ANDROID;
            } else {
                this.osName = "unknown";
            }
        }
    }

    private static String getFieldInfoByKey(String field, String[] rawFieldArray) {

        if (StringUtils.isBlank(field) || !FIELD_MAP.containsKey(field)
                || Objects.isNull(rawFieldArray) || rawFieldArray.length == 0) {
            return null;
        }

        int fieldIdx = FIELD_MAP.get(field);
        return rawFieldArray.length > fieldIdx && fieldIdx >= 0 ? rawFieldArray[fieldIdx] : null;
    }

    private void clear() {
        this.appName = null;
        this.appVersion = null;
        this.osName = null;
        this.osVersion = null;
        this.manufacturer = null;
        this.model = null;
    }

    public static CustomUserAgentInfo getThreadLocal(String customUserAgent) {
        val customUserAgentInfo = CUSTOM_USER_AGENT_INFO.get();
        customUserAgentInfo.clear();
        customUserAgentInfo.refresh(customUserAgent);
        return customUserAgentInfo;
    }

}
