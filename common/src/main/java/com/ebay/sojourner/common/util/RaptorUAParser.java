package com.ebay.sojourner.common.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ebay.platform.dds.api.DeviceInfoProvider;
import com.ebay.platform.dds.data.DeviceDataConsumer;
import com.ebay.platform.raptor.ddsmodels.DeviceInfo;
import com.ebay.sojourner.common.env.EnvironmentUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaptorUAParser extends AbstractUAParser {

  public static final Charset CHARSET = UTF_8;
  private static final Long ONE_MINUTE = 60 * 1000L;
  private static final Long ONE_HOUR = 60 * 60 * 1000L;
  private static final int INIT_CACHE_SIZE = 10000;
  private static final int CACHE_SIZE = 50000;
  private static String consumerAppName = "PulsarStreamConsumer";
  private static volatile RaptorUAParser raptorUAParser = new RaptorUAParser(EnvironmentUtils.get(
      "ua-parser-env"));
  private DeviceInfoProvider deviceInfoProviderInstance;
  private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
  private HashFunction hashFunc = Hashing.murmur3_128(2147368987);
  private Cache<HashCode, Map<String, String>> uaParserCache = CacheBuilder.newBuilder()
      .initialCapacity(INIT_CACHE_SIZE).concurrencyLevel(8).maximumSize(CACHE_SIZE).build();

  private RaptorUAParser(String env) {
    DeviceDataConsumer.registerConsumer(consumerAppName);
    deviceInfoProviderInstance = new DeviceInfoProvider(env, false);
    timer.scheduleWithFixedDelay(new UARefresher(), ONE_MINUTE, ONE_HOUR, TimeUnit.MILLISECONDS);
  }

  public static RaptorUAParser getInstance() {
    if (raptorUAParser == null) {
      synchronized (RaptorUAParser.class) {
        if (raptorUAParser == null) {
          raptorUAParser = new RaptorUAParser(EnvironmentUtils.get(
              "ua-parser-env"));
        }
      }
    }
    return raptorUAParser;
  }


  public Map<String, String> processUA(String agent, String dn, boolean isPulsar) {
    if (agent == null || agent.trim().length() == 0) {
      return UAConstants.OTHERRESULT;
    }
    //This is an method from from old Guava lib, use this to be compatible with jetstream's Guava.
    HashCode hashCode = hashFunc.hashString(agent, CHARSET);
    Map<String, String> uaResult = (Map<String, String>) uaParserCache.getIfPresent(hashCode);
    if (uaResult == null) {
      cachemiss.incrementAndGet();
      DeviceInfo deviceInfo = deviceInfoProviderInstance.get(agent);
      if (deviceInfo != null) {
        uaResult = new HashMap<String, String>();
        if (deviceInfo.getOsName() != null) {
          uaResult.put(UAConstants.DEVICETYPE, deviceInfo.getOsName());
        }
        if (deviceInfo.isTablet()) {
          uaResult.put(UAConstants.DEVICEFAMILY, UAConstants.DEVICE_TABLET);
        } else if (deviceInfo.isTouchScreen()) {
          uaResult.put(UAConstants.DEVICEFAMILY, UAConstants.DEVICE_TOUCH_SCREEN);
        } else if (deviceInfo.isDesktop()) {
          uaResult.put(UAConstants.DEVICEFAMILY, UAConstants.DEVICE_DESKTOP);
        } else if (deviceInfo.isMobile()) {
          uaResult.put(UAConstants.DEVICEFAMILY, UAConstants.DEVICE_MOBILE);
        } else {
          uaResult.put(UAConstants.DEVICEFAMILY, UAConstants.OTHER);
        }
        if (deviceInfo.getDeviceOS() != null) {
          uaResult.put(UAConstants.OSFAMILY, deviceInfo.getDeviceOS());
          uaResult.put(UAConstants.OSVESION, deviceInfo.getDeviceOSVersion());
        } else {
          uaResult.put(UAConstants.OSFAMILY, UAConstants.OTHER);
        }
        if (deviceInfo.getBrowser() != null) {
          uaResult.put(UAConstants.BROWSERFAMILY, deviceInfo.getBrowser());
          uaResult.put(UAConstants.BROWSERVESION, deviceInfo.getBrowserVersion());
        } else {
          uaResult.put(UAConstants.BROWSERFAMILY, UAConstants.OTHER);
        }
        // Tune the device result of the Agent Parser
        tuneParseResult(agent, dn, uaResult, isPulsar);
        uaParserCache.put(hashCode, uaResult);
        return uaResult;
      } else {
        if (isPulsar) {
          return UAConstants.OTHERRESULT_PULSAR;
        } else {
          return UAConstants.OTHERRESULT;
        }
      }
    } else {
      cachehit.incrementAndGet();
      return uaResult;
    }
  }

  public void refresh() {
    if (deviceInfoProviderInstance != null) {
      try {
        deviceInfoProviderInstance.refresh();
      } catch (Exception e) {
        log.warn("UARefresher failed: Exception= " + e.getMessage());
      }
    }
  }

  private class UARefresher implements Runnable {

    public void run() {
      Calendar cal = Calendar.getInstance();
      if (cal.get(Calendar.HOUR_OF_DAY) == 5) {
        refresh();
      }
    }
  }
}