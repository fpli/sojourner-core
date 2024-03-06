package com.ebay.sojourner.flink.connector.kafka.rheos;

import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.connector.kafka.rheos.pojo.RheosStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.auth.SecurityProtocol;

@Slf4j
public class RheosStreamsConfig {

  public static Map<String, List<RheosStream>> rheosStreams = new HashMap<>(2);

  private static final String PROD = "prod";
  private static final String STAGE = "stage";
  private static final String PROD_CONFIG_FILE = String.format("rheos-stream-%s.yaml", PROD);
  private static final String STAGE_CONFIG_FILE = String.format("rheos-stream-%s.yaml", STAGE);


  private RheosStreamsConfig() {
    // intended to be empty
  }

  static {
    ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    try {
      // load prod rheos streams
      InputStream prod = RheosStreamsConfig.class.getClassLoader().getResourceAsStream(PROD_CONFIG_FILE);
      if (prod == null) {
        throw new RheosStreamConfigException("Cannot find " + PROD_CONFIG_FILE);
      }
      log.info("Load prod rheos stream info");
      List<RheosStream> prodRheosStream = objectMapper.readValue(prod, new TypeReference<List<RheosStream>>() {
      });
      rheosStreams.put(PROD, prodRheosStream);

      // load stage rheos streams
      InputStream stage = RheosStreamsConfig.class.getClassLoader().getResourceAsStream(STAGE_CONFIG_FILE);
      if (stage == null) {
        throw new RheosStreamConfigException("Cannot find " + STAGE_CONFIG_FILE);
      }
      log.info("Load stage rheos stream info");
      List<RheosStream> stageRheosStream = objectMapper.readValue(stage, new TypeReference<List<RheosStream>>() {
      });
      rheosStreams.put(STAGE, stageRheosStream);

    } catch (IOException e) {
      throw new RheosStreamConfigException(e);
    }

  }

  public static List<String> getBrokerList(String env, String stream, DataCenter dataCenter) {
    RheosStream rheosStream = getStream(env, stream);

    if (dataCenter == null) {
      if (rheosStream.getBrokers().size() == 1) {
        for (List<String> value : rheosStream.getBrokers().values()) {
          return value;
        }
      } else {
        throw new RheosStreamConfigException(
            String.format("Stream %s has multiple DC setup, you must select a specific DC", stream));
      }
    }

    Preconditions.checkArgument(rheosStream.getBrokers().containsKey(dataCenter),
        String.format("Stream %s does not contain DC %s brokers config", stream, dataCenter));

    return rheosStream.getBrokers().get(dataCenter);
  }

  public static List<String> getBrokerList(String stream, DataCenter dataCenter) {
    Preconditions.checkNotNull(stream);

    return getBrokerList(PROD, stream, dataCenter);
  }

  public static List<String> getBrokerList(String stream) {
    Preconditions.checkNotNull(stream);

    return getBrokerList(stream, null);
  }

  public static String getBrokersAsString(String env, String stream, DataCenter dataCenter) {
    return String.join(",", getBrokerList(env, stream, dataCenter));
  }

  public static String getBrokersAsString(String stream, DataCenter dataCenter) {
    return String.join(",", getBrokerList(stream, dataCenter));
  }

  public static String getBrokersAsString(String stream) {
    return String.join(",", getBrokerList(stream));
  }

  public static RheosStream getStream(String env, String streamName) {
    Preconditions.checkArgument(rheosStreams.containsKey(env), "Cannot find env %s", env);

    List<RheosStream> streams = rheosStreams.get(env);
    return streams.stream()
                  .filter(s -> s.getName().equalsIgnoreCase(streamName))
                  .findFirst()
                  .orElseThrow(() -> new IllegalArgumentException("Cannot find stream " + streamName));
  }

  /**
   * If env is not provided, use `prod` as default env
   */
  public static RheosStream getStream(String streamName) {

    return getStream(PROD, streamName);
  }

  public static SecurityProtocol getAuthProtocol(String env, String stream) {
    RheosStream rheosStream = getStream(env, stream);
    return rheosStream.getAuthProtocol();
  }

  public static SecurityProtocol getAuthProtocol(String stream) {
    RheosStream rheosStream = getStream(stream);
    return rheosStream.getAuthProtocol();
  }

}
