package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimpleDeviceExperienceDetectionStrategyTest {
    static SimpleDeviceExperienceDetectionStrategy strategy;

    @BeforeAll
    static void beforeAll() {
        strategy = new SimpleDeviceExperienceDetectionStrategy();
    }

    @Test
    void detectDevice() {
        UbiEvent ubiEvent = new UbiEvent();
        val context = SignalContext.getThreadLocalContext(new RawEvent(), ubiEvent);

        int testRounds = 10_000;

        val stopwatch = Stopwatch.createStarted();

        HashMap<String, String> uaList = Maps.newHashMap();
        uaList.put("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;", "android");
        uaList.put("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36", "dweb");
        uaList.put("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/109.0.5414.118 Safari/534.24 XiaoMi/MiuiBrowser/18.1.110304 swan-mibrowser", "dweb"); // FIXME Should be mweb
        uaList.put("ebayUserAgent/eBayIOS;6.151.0;iOS;17.2;Apple;iMac21_2;--;900x1600;2.0", "ios");
        uaList.put("ebayUserAgent/eBayIOS;6.103.0;iOS;15.6.1;Apple;iPad13_1;no-carrier;820x1180;2.0", "ipad");
        uaList.put("ebayUserAgent/eBayIOS;6.140.0;iOS;17.3.1;Apple;iPhone14_7;--;390x844;3.0", "iphone");
        uaList.put("Mozilla/5.0 (Windows NT 10.0; Win64; x64; WebView/3.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19045", "mweb");
        uaList.put("Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1", "mweb");
        uaList.put("eBayiPhone/5.17.0", "ios"); // Legacy UA for coverage report. FIXME should be iphone
        uaList.put("eBayAndroid/5.10.0.11", "android"); // Legacy UA for coverage report
        uaList.put("eBayMotorsiPhone/2.0.4", "unknown"); // Legacy UA for coverage report. FIXME should be iphone
        uaList.put("eBayMotors/2.0.4", "unknown"); // Legacy UA for coverage report
        uaList.put("eBayMotorsiPhone%%%%%%%%2/2.0.4", "mweb"); // Wrongly encoded for coverage report
        uaList.put("ebayUserAgent/eBayIOS;6.140.0;;17.3.1;Apple;iPhone14_7;--;390x844;3.0", "iphone"); // Wrong UA for coverage report
        uaList.put("ebayUserAgent/eBayAndroid;6.151.1;;13;Google;coral;Xfinity Mobile;1440x2872;3.5;", "android"); // Wrong UA for coverage report

        IntStream.range(0, testRounds).forEach(i -> uaList.forEach((key, value) -> {
            ubiEvent.setAgentInfo(key);
            assertEquals(value, strategy.detectDevice(context));
        }));

        long nano = stopwatch.stop().elapsed().toNanos();
        System.out.println("Elapsed: " + nano + " ns");
        System.out.println("Avg: " + nano / testRounds / uaList.size() + " ns");
    }
}
