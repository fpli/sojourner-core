package com.ebay.sojourner.cjs.util.dds;

import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static com.ebay.sojourner.cjs.util.dds.DDSConstants.ANDROID;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.CUSTOM_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_ANDROID_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_FASHION_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_IPAD_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_IPHONE_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_MOTORSIP_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_MOTORS_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_SELLINGIP_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_WINCORE_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.EBAY_WINPHO_UA_PREFIX;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.IOS;

public class DeviceInfoDelegate {

    public static String tryNormalize(String raw) {

        try {
            String decoded = URLDecoder.decode(StringUtils.trimToEmpty(raw), StandardCharsets.UTF_8.name());
            return StringUtils.strip(decoded, "\"' ");
        } catch (Exception e) {
            // fallback without decode
            return raw;
        }
    }

    public static CustomUserAgentInfo getInfoByCustomAgent(String customAgent) {
        return CustomUserAgentInfo.getThreadLocal(customAgent);
    }

    public static UaType getUaType(String userAgent) {
        if (userAgent.startsWith(CUSTOM_UA_PREFIX)) {
            return UaType.CUSTOM_UA_PREFIX;
        }

        if (userAgent.toLowerCase().startsWith(EBAY_IPHONE_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_ANDROID_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_IPAD_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_FASHION_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_WINPHO_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_MOTORSIP_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_WINCORE_UA_PREFIX) ||
                userAgent.toLowerCase().startsWith(EBAY_SELLINGIP_UA_PREFIX)) {

            return UaType.EBAY_NATIVE;
        }

        if (userAgent.toLowerCase().startsWith(EBAY_MOTORS_UA_PREFIX)) {
            return UaType.EBAY_NATIVE;
        }

        return UaType.DEFAULT;
    }

    /**
     * <pre>
     * Convert old format UA to new format UA Old Format UA Deprecated APP?
     * Converted as New Format UA userAgentInfo.getAppInfo().getAppName()
     * eBayiPhone/5.17.0 ebayUserAgent/eBayIOS;5.17.0 eBayIOS eBayAndroid/5.10.0.11
     * ebayUserAgent/eBayAndroid;5.10.0.11 eBayAndroid eBayiPad/5.1.0
     * ebayUserAgent/eBayIOS;5.1.0 eBayIOS eBayFashion/1.9.0
     * ebayUserAgent/eBayIOS;1.9.0 eBayIOS eBayWinPhoCore/1.5 Deprecated
     * ebayUserAgent/unknown;1.5 unknown eBayMotorsiPhone/2.0.4 Deprecated
     * ebayUserAgent/unknown;2.0.4 unknown eBayWinCore/1.2 Deprecated
     * ebayUserAgent/unknown;1.2 unknown eBaySellingiPhone/1.1.1 Deprecated
     * ebayUserAgent/unknown;1.1.1 unknown
     * </pre>
     */
    public static String convertToNewFormatUA(String userAgent) {
        String newFormatUAStr;
        String lowerCaseUA = userAgent.toLowerCase();
        if (lowerCaseUA.startsWith(EBAY_IPAD_UA_PREFIX.toLowerCase()) ||
                lowerCaseUA.startsWith(EBAY_FASHION_UA_PREFIX.toLowerCase()) ||
                lowerCaseUA.startsWith(EBAY_IPHONE_UA_PREFIX.toLowerCase())) {

            newFormatUAStr = CUSTOM_UA_PREFIX + "/eBayIOS;" + userAgent.substring(userAgent.indexOf("/") + 1);
        } else if (lowerCaseUA.startsWith(EBAY_ANDROID_UA_PREFIX.toLowerCase())) {
            newFormatUAStr = CUSTOM_UA_PREFIX + "/" + userAgent.replaceFirst("/", ";");
        } else if (lowerCaseUA.startsWith(EBAY_MOTORS_UA_PREFIX.toLowerCase())) {
            // e.g. eBayMotors/2.93.0 (ios) Dart/3.0.3 Build/2.93.0.9483
            newFormatUAStr =
                    CUSTOM_UA_PREFIX + "/" +
                            userAgent.replaceFirst("/", ";")
                                     .replaceAll("\\s*\\(|\\).*", ";")
                                     .replaceFirst("(?i)" + IOS, IOS)
                                     .replaceFirst("(?i)" + ANDROID, ANDROID);
        } else {
            newFormatUAStr = CUSTOM_UA_PREFIX + "/unknown;" +
                    userAgent.substring(userAgent.indexOf("/") + 1);
        }
        return newFormatUAStr;
    }

}
