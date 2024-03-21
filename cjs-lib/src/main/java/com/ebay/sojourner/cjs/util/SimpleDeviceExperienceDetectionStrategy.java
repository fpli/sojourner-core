package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.cjs.util.dds.UaType;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import static com.ebay.sojourner.cjs.util.dds.DDSConstants.ANDROID;
import static com.ebay.sojourner.cjs.util.dds.DDSConstants.IOS;
import static com.ebay.sojourner.cjs.util.dds.DeviceInfoDelegate.convertToNewFormatUA;
import static com.ebay.sojourner.cjs.util.dds.DeviceInfoDelegate.getInfoByCustomAgent;
import static com.ebay.sojourner.cjs.util.dds.DeviceInfoDelegate.getUaType;
import static com.ebay.sojourner.cjs.util.dds.DeviceInfoDelegate.tryNormalize;
import static org.apache.commons.lang3.StringUtils.trimToEmpty;

public class SimpleDeviceExperienceDetectionStrategy implements DeviceExperienceDetectionStrategy {

    @Override
    public String detectDevice(SignalContext context) {
        // This is extracted by rawEvent.getClientData().getAgent()
        val userAgent = trimToEmpty(tryNormalize(context.getUbiEvent().getAgentInfo()));

        if (StringUtils.isBlank(userAgent)) {
            return "unknown";
        }

        UaType uaType = getUaType(userAgent);

        if (uaType == UaType.DEFAULT) { // Browser
            return isMobile(userAgent) ? "mweb" : "dweb";
        }

        // Native
        val customInfo = getInfoByCustomAgent(
                uaType == UaType.EBAY_NATIVE ? convertToNewFormatUA(userAgent) : userAgent
        );

        val os = customInfo.getOsName();
        if (!StringUtils.equals(os, IOS)) {
            if (StringUtils.equals(os, ANDROID)) {
                return "android";
            } else {
                return "unknown";
            }
        }

        if (StringUtils.isNotBlank(customInfo.getModel())) {
            if (customInfo.getModel().toLowerCase().contains("ipad")) {
                return "ipad";
            } else if (customInfo.getModel().toLowerCase().contains("iphone")) {
                return "iphone";
            }
        }

        return "ios";
    }

    private static boolean isMobile(String ua) {
        if (StringUtils.isBlank(ua)) {
            return false;
        }

        return StringUtils.containsIgnoreCase(ua, "WebView") ||
                ua.contains("Android") ||
                ua.contains("webOS") ||
                ua.contains("iPhone") ||
                ua.contains("iPad") ||
                ua.contains("iPod") ||
                ua.contains("BlackBerry") ||
                ua.contains("Windows Phone");
    }

}