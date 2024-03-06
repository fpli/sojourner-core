package com.ebay.sojourner.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class BotHostMatcher {

  public static final BotHostMatcher INSTANCE = new BotHostMatcher();
  public static final String LINUX_NS_LOOKUP = "nslookup";
  public static final String DNS_KEY = "name =";
  public static final long THRESHOLD = 100000;
  public static final int RETRY_THRESHOLD_EACH_IP = 3;
  public static final String AMAZON_AWS = "amazonaws.";
  public static final String THE_PLANET = ".theplanet.";
  public static final String GI_XEN = ".gixen.com";
  public static final String POST_NEWS = "postnews";
  public static final String GOOGLE_APP = ".google.";
  public static Map<String, Boolean> hostBotMap = new ConcurrentHashMap<String, Boolean>();
  // public static Map<String, String> ipHostMap = new ConcurrentHashMap<String, String>();

  private Runtime runtime;

  public BotHostMatcher() {
    this.runtime = Runtime.getRuntime();
  }

  public boolean isBotIp(String ip) {
    if (ip == null) {
      return false;
    }
    //        String host = ipHostMap.get(ip);
    //        if(host == null) {
    //            host = getHostByIp(ip, 0);
    //            if(host == null) {
    //                host = "";
    //            }
    //            ipHostMap.put(ip, host);
    //        }
    return isBotHost(getHostByIp(ip, 0));
  }

  public Boolean getBotHostFromCache(String host) {
    return hostBotMap.get(host);
  }

  public void setBotHostToCache(String ip, Boolean status) {
    if (hostBotMap.size() < THRESHOLD) {
      hostBotMap.put(ip, status);
    }
  }

  //  public String getHostFromCache(String ip) {
  //    return ipHostMap.get(ip);
  //  }
  //
  //  public void setHostToCache(String ip, String host) {
  //    if (ipHostMap.size() < THRESHOLD) {
  //      ipHostMap.put(ip, host);
  //    }
  //  }

  public String getHostByIp(String ip, int retryNumber) {
    String host = null;
    try {
      String[] ips = new String[]{LINUX_NS_LOOKUP, ip};
      Process proc = runtime.exec(ips);
      String result = getHostFromStdout(proc);
      printStdError(proc);
      if (StringUtils.isNotBlank(result)) {
        host = result.substring(result.indexOf(DNS_KEY) + DNS_KEY.length());
        if (host != null) {
          host = host.trim();
        }
      }
      log.info("Process exitValue: " + proc.waitFor() + " ip: " + ip);
    } catch (Exception e) {
      throw new RuntimeException("host: " + host, e);
    }

    boolean isIp = IsValidIPv4.isValidIP(host);
    //        if (retryNumber > 0 && !isIp) {
    //            botContext.increment(Counter.SUCCESS_GET_HOST_RETRY, 1);
    //        }

    if (retryNumber < RETRY_THRESHOLD_EACH_IP && isIp) {
      //            if (retryNumber == 0) {
      //                botContext.increment(Counter.STILL_IP, 1);
      //            }
      retryNumber++;
      host = getHostByIp(ip, retryNumber);
    }

    return host;
  }

  private void printStdError(Process proc) throws IOException {
    BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    String stdError = null;
    boolean error = false;
    while ((stdError = errReader.readLine()) != null) {
      log.error(stdError);
      error = true;
    }

    //        if (error) {
    //            botContext.increment(Counter.ERROR_HOST, 1);
    //        }
  }

  private String getHostFromStdout(Process proc) throws IOException {
    String hostInfo = null;
    String stdMsg = null;
    BufferedReader stdReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    while ((stdMsg = stdReader.readLine()) != null) {
      if (stdMsg.contains(DNS_KEY)) {
        hostInfo = stdMsg;
        // Continue for preventing dead lock on external program output buffer
      }
    }
    return hostInfo;
  }

  public boolean isBotHost(String host) {
    if (StringUtils.isNotBlank(host)) {
      return host.contains(AMAZON_AWS)
          || host.contains(THE_PLANET)
          || host.contains(GI_XEN)
          || (host.startsWith(POST_NEWS) && host.contains(GOOGLE_APP));
    }

    return false;
  }
}
