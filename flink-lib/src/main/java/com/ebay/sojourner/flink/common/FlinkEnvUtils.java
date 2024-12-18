package com.ebay.sojourner.flink.common;

import static com.ebay.sojourner.common.util.Property.CHECKPOINT_INTERVAL_MS;
import static com.ebay.sojourner.common.util.Property.CHECKPOINT_MAX_CONCURRENT;
import static com.ebay.sojourner.common.util.Property.CHECKPOINT_MIN_PAUSE_BETWEEN_MS;
import static com.ebay.sojourner.common.util.Property.CHECKPOINT_TIMEOUT_MS;

import com.ebay.sojourner.common.env.EnvironmentUtils;
import com.ebay.sojourner.flink.state.StateBackendFactory;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Deprecated
@Slf4j
public class FlinkEnvUtils {

  private static final Map<String, String> CONFIG = Maps.newHashMap();

  private static final String PROFILE = "profile";

  private static void load(String[] args) {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String profile = parameterTool.get(PROFILE);
    if (StringUtils.isNotBlank(profile)) {
      CONFIG.put(PROFILE, profile);
      EnvironmentUtils.activateProfile(profile);
    }
    EnvironmentUtils.fromProperties(parameterTool.getProperties());

    // load git.properties file if exists
    try (InputStream input = FlinkEnvUtils.class.getClassLoader()
                                                .getResourceAsStream("git.properties")) {
      Properties prop = new Properties();

      if (input == null) {
        log.info("Not found git.properties file");
        return;
      }

      //load git.properties and put props into CONFIG map
      prop.load(input);
      for (String key : prop.stringPropertyNames()) {
        CONFIG.put(key, prop.getProperty(key));
      }
    } catch (IOException e) {
      log.error("Error when loading git.properties file", e);
    }
  }

  public static StreamExecutionEnvironment prepare(String[] args) {
    load(args);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().disableAutoGeneratedUIDs();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // checkpoint config
    // create a checkpoint every 5 minutes
    env.enableCheckpointing(getInteger(CHECKPOINT_INTERVAL_MS));
    CheckpointConfig conf = env.getCheckpointConfig();
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    conf.setMinPauseBetweenCheckpoints(getInteger(CHECKPOINT_MIN_PAUSE_BETWEEN_MS));//2min
    conf.setCheckpointTimeout(getInteger(CHECKPOINT_TIMEOUT_MS));//15min
    conf.setMaxConcurrentCheckpoints(getInteger(CHECKPOINT_MAX_CONCURRENT));

    // set tolerable checkpoint failure number to Integer.MAX_VALUE to prevent flink job restart
    conf.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);

    // state config
    env.setStateBackend(StateBackendFactory.getStateBackend(StateBackendFactory.ROCKSDB));

    return env;
  }

  // https://stackoverflow.com/questions/51417258/how-to-increase-flink-taskmanager-numberoftaskslots-to-run-it-without-flink-serv
  public static StreamExecutionEnvironment prepareLocal(String[] args) {
    load(args);
    final Configuration configuration = new Configuration();
    configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 10);
    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
    env.getConfig().disableAutoGeneratedUIDs();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // checkpoint config
    // create a checkpoint every 5 minutes
    env.enableCheckpointing(getInteger(CHECKPOINT_INTERVAL_MS));
    CheckpointConfig conf = env.getCheckpointConfig();
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    conf.setMinPauseBetweenCheckpoints(getInteger(CHECKPOINT_MIN_PAUSE_BETWEEN_MS));//2min
    conf.setCheckpointTimeout(getInteger(CHECKPOINT_TIMEOUT_MS));//15min
    conf.setMaxConcurrentCheckpoints(getInteger(CHECKPOINT_MAX_CONCURRENT));

    // state config
    env.setStateBackend(StateBackendFactory.getStateBackend(StateBackendFactory.HASHMAP));

    return env;
  }

  public static void execute(StreamExecutionEnvironment env, String jobName) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromMap(CONFIG);
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(parameterTool);
    env.execute(jobName);
  }

  public static String getString(String key) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return value;
  }

  public static String getStringOrDefault(String key, String defaultValue) {
    String value = EnvironmentUtils.getStringOrDefault(key, defaultValue);
    CONFIG.put(key, value);
    return value;
  }

  public static Integer getInteger(String key) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return Integer.valueOf(value);
  }

  public static Long getLong(String key) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return Long.valueOf(value);
  }

  public static Boolean getBoolean(String key) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return Boolean.valueOf(value);
  }

  public static String getListString(String key) {
    List<String> list = EnvironmentUtils.getForClass(key, List.class);
    String value = String.join(",", list);
    CONFIG.put(key, value);
    return value;
  }

  public static Set<String> getSet(String key) {
    List<String> list = EnvironmentUtils.getForClass(key, List.class);
    String value = String.join(",", list);
    CONFIG.put(key, value);
    return new HashSet<>(list);
  }

  public static List<String> getList(String key) {
    List<String> list = EnvironmentUtils.getForClass(key, List.class);
    String value = String.join(",", list);
    CONFIG.put(key, value);
    return list;
  }

  public static String[] getStringArray(String key, String delimiter) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return EnvironmentUtils.getStringArray(key, delimiter);
  }

  public static List<String> getStringList(String key, String delimiter) {
    String value = EnvironmentUtils.get(key);
    CONFIG.put(key, value);
    return EnvironmentUtils.getStringList(key, delimiter);
  }
}
