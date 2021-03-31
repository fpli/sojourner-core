package com.ebay.sojourner.common.util;

public class Property {

  // --------------------- UBI Property File -------------------------------------
  // Session properties
  public static final String SESSION_IDLE_TIMEOUT = "session.idleTimeOut";
  public static final String SESSION_MAX_DURATION = "session.maxDuration";
  // Event properties
  public static final String EVENT_WAITING_PERIOD = "event.waitingPeriod";
  public static final String EVENT_POOL_THRESHOLD = "event.pool.threshold";
  public static final String EVENT_DELAY_THRESHOLD = "event.delay.threshold";

  // -------------------- Lookup Property File ----------------------------------
  // Lookup table properties
  public static final String IFRAME_PAGE_IDS = "iframe.page.ids";
  public static final String IFRAME_PAGE_IDS4Bot12 = "iframe.page.ids.bot12";
  public static final String FINDING_FLAGS = "finding.flags";
  public static final String VTNEW_IDS = "vtNewIds";
  public static final String IAB_AGENT = "iab.agent";
  public static final String APP_ID = "appid";
  public static final String PAGE_FMLY = "pagefmly";
  public static final String TEST_USER_IDS = "test.user.ids";
  public static final String LARGE_SESSION_GUID = "large.session.guid";
  public static final String MPX_ROTATION = "mpx.rotation";
  public static final String PAGE_FMLY_ALL = "pageFmlyAll";

  // -------------------- Page Indicator Property --------------------------------
  public static final String SEARCH_VIEW_PAGES = "search.view.pages";
  public static final String VIEW_ITEM_PAGES = "view.item.pages";
  public static final String BID_PAGES = "bid.pages";
  public static final String BIN_PAGES = "bin.pages";
  public static final String CAPTCHA_PAGES = "captcha.pages";
  public static final String PROPERTY_DELIMITER = ",";
  public static final String HALF_PAGES = "half.pages";
  public static final String CORESITE_PAGES = "coresite.pages";
  public static final String CLASSIFIED_PAGES = "classified.pages";
  public static final String SCEVENT_EXCLUDE_PAGES1 = "scevent.exclude.pages1";
  public static final String SCEVENT_EXCLUDE_PAGES2 = "scevent.exclude.pages2";
  public static final String ROVER_PAGES = "rover.pages";
  public static final String LAND_PAGES1 = "land.pages1";
  public static final String LAND_PAGES2 = "land.pages2";
  public static final String AGENT_EXCLUDE_PAGES = "agent.exclude.pages";
  public static final String NOTIFY_CLICK_PAGES = "notify.click.pages";
  public static final String NOTIFY_VIEW_PAGES = "notify.view.pages";
  public static final String SOCIAL_AGENT_ID22 = "social.agent.ids22";
  public static final String SOCIAL_AGENT_ID23 = "social.agent.ids23";
  public static final String MOBILE_PAGES = "mobile.pages";
  // -------------------- New BOT Property --------------------------------
  public static final String INVALID_PAGE_IDS = "invalid.page.ids";
  public static final String LNDG_PAGE_IDS = "lndg.page.ids";
  public static final String EBAY_SITE_COBRAND = "ebay.site.cobrand";
  public static final String MKTG_TRAFFIC_SOURCE_IDS = "mktg.traffic.source.ids";
  public static final String BROWSER_AGENT_STRING = "browser.agent.string";
  public static final String BOT_AGENT_STRING = "bot.agent.string";
  public static final String IP_EXCLUDE_PAGES = "ip.exclude.pages";
  public static final String EXCLUDE_IP_PATTERN = "exclude.ip.pattern";
  public static final String EBAY_NONBROWSER_COBRAND = "ebay.nonbrower.cobrand";
  public static final String PAGE_SESSION_CLICKS = "page.session.clicks";
  public static final String INVALID_BOT_FILTER = "invalid.session.bot.filter";
  public static final String SELECTED_IPS = "selected.ips";
  public static final String SELECTED_AGENTS = "selected.agents";
  public static final String ITM_PAGES = "itm.pages";
  // --------------------- APP PAYLOAD KV Property ---------------------------------
  public static final String SWD_VALUES = "swd.values";
  public static final String ROT_VALUES = "rot.values";
  public static final String VI_EVENT_VALUES = "event.pgt.vi";
  public static final String PRELOAD_PAYLOAD_TAGS = "preload.payload.tags";

  // --------------------- APP ID Property -----------------------------------------
  public static final String MOBILE_APP = "mobile.appids";
  public static final String DESKTOP_APP = "desktop.appids";
  public static final String EIM_APP = "eim.appids";

  // -------------------- BOT Rule Property ---------------------------------------
  public static final String BOT_RULE_PACKAGE = "bot.rule.package";
  public static final String BOT_RULE_CLASSES = "bot.rule.classes";
  public static final String BOT_EOD_UPDATE = "bot.eod.update";
  public static final String BOT_DELIMITER = ",";

  // ----------------------------  Cobrand Property  ------------------------------
  public static final String EXPRESS_SITE = "express.site";
  public static final String HALF_SITE = "half.site";
  public static final String EXPRESS_PARTNER = "express.partner";
  public static final String SHOPPING_PARTNER = "shopping.partner";
  public static final String HALF_PARTNER = "half.partner";
  public static final String ARTISAN_PARTNER = "artisan.partner";
  public static final String MOBILE_AGENT_START = "mobile.agent.start.pattern";
  public static final String MOBILE_AGENT_INDEX = "mobile.agent.index.pattern";
  public static final String MOBILE_AGENT_OTHER = "mobile.agent.other.pattern";
  public static final String MOBILE_AGENT_DELIMITER = "#";
  public static final String START_IDENTIFIER = "^";
  public static final String LAST_UOW = "event_data.lastuow";
  // --------------------- Filter Name List Property ------------------------------
  public static final String DISABLED_FILTER_NAMES = "disabled.filter.names";

  // ---------------------- LOG Property -----------------------------------------
  public static final String LOG_LEVEL = "log.level";
  public static final String DEFAULT_LOG_LEVEL = "INFO";

  // ---------------------- Track and Monitor -----------------------------------------
  public static final String TASK_TRACK_PERIOD = "task.track.period";

  // ---------------------- LARGE_SESSION  -----------------------------------------
  public static final String LARGE_SESSION_EVENT_NUMBER = "event.number.threshold";
  public static final String LARGE_SESSION_TIMES_OF_BOT15 = "times.of.bot15";

  // ---------------------- Enable com.ebay.sojourner.ubd.common.util.Test
  // -----------------------------------------
  public static final String IS_TEST_ENABLE = "enable.test";

  public static final String LKP_PATH = "lkpPath";

  // --------------------- common config property ------------------------------
  // kafka consumer
  public static final String MAX_POLL_RECORDS = "kafka.consumer.max-poll-records";
  public static final String RECEIVE_BUFFER = "kafka.consumer.receive-buffer";
  public static final String FETCH_MAX_BYTES = "kafka.consumer.fetch-max-bytes";
  public static final String FETCH_MAX_WAIT_MS = "kafka.consumer.fetch-max-wait-ms";
  public static final String MAX_PARTITIONS_FETCH_BYTES = "kafka.consumer.max-partitions-fetch-bytes";
  public static final String AUTO_RESET_OFFSET = "kafka.consumer.auto-offset-reset";
  public static final String KAFKA_CONSUMER_BOOTSTRAP_SERVERS = "kafka.consumer.bootstrap-servers";
  public static final String KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
  public static final String KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group-id";

  // kafka producer
  public static final String BATCH_SIZE = "kafka.producer.batch-size";
  public static final String REQUEST_TIMEOUT_MS = "kafka.producer.request-timeout-ms";
  public static final String DELIVERY_TIMEOUT_MS = "kafka.producer.delivery-timeout-ms";
  public static final String REQUEST_RETRIES = "kafka.producer.retries";
  public static final String LINGER_MS = "kafka.producer.linger-ms";
  public static final String BUFFER_MEMORY = "kafka.producer.buffer-memory";
  public static final String ACKS = "kafka.producer.acks";
  public static final String COMPRESSION_TYPE = "kafka.producer.compression-type";
  public static final String KAFKA_PRODUCER_BOOTSTRAP_SERVERS = "kafka.producer.bootstrap-servers";
  public static final String PRODUCER_ID = "kafka.producer.producerId";

  // rheos
  public static final String RHEOS_KAFKA_REGISTRY_URL = "rheos.registry-url";
  public static final String RHEOS_CLIENT_ID = "rheos.client.id";
  public static final String RHEOS_CLIENT_IAF_SECRET = "rheos.client.iaf.secret";
  public static final String RHEOS_CLIENT_IAF_ENV = "rheos.client.iaf.env";

  // flink - app name
  public static final String FLINK_APP_NAME = "flink.app.name";

  // flink source
  public static final String FLINK_APP_SOURCE_DC = "flink.app.source.dc";
  public static final String FLINK_APP_SOURCE_OP_NAME = "flink.app.source.operator-name";
  public static final String FLINK_APP_SOURCE_FROM_TIMESTAMP = "flink.app.source.from-timestamp";
  public static final String FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN = "flink.app.source.out-of-orderless-in-min";
  public static final String FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN = "flink.app.source.idle-source-timeout-in-min";

  // flink sink
  public static final String FLINK_APP_SINK_DC = "flink.app.sink.dc";
  public static final String FLINK_APP_SINK_OP_NAME = "flink.app.sink.operator-name";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC = "flink.app.sink.kafka.topic";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_SESSION_BOT = "flink.app.sink.kafka.topic.session.bot";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_SESSION_NON_BOT = "flink.app.sink.kafka.topic.session.non-bot";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_EVENT_BOT = "flink.app.sink.kafka.topic.event.bot";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_EVENT_NON_BOT = "flink.app.sink.kafka.topic.event.non-bot";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_EVENT_LATE = "flink.app.sink.kafka.topic.event.late";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_SIGNATURE_AGENT_IP = "flink.app.sink.kafka.topic.signature.agent-ip";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_SIGNATURE_AGENT = "flink.app.sink.kafka.topic.signature.agent";
  public static final String FLINK_APP_SINK_KAFKA_TOPIC_SIGNATURE_IP = "flink.app.sink.kafka.topic.signature.ip";
  public static final String FLINK_APP_SINK_KAFKA_SUBJECT_EVENT = "flink.app.sink.kafka.subject.event";
  public static final String FLINK_APP_SINK_KAFKA_SUBJECT_SESSION = "flink.app.sink.kafka.subject.session";
  public static final String FLINK_APP_SINK_KAFKA_MESSAGE_KEY_EVENT = "flink.app.sink.kafka.message-key.event";
  public static final String FLINK_APP_SINK_KAFKA_MESSAGE_KEY_SESSION = "flink.app.sink.kafka.message-key.session";
  public static final String FLINK_APP_SINK_KAFKA_MESSAGE_KEY_SIGNATURE_AGENT_IP = "flink.app.sink.kafka.message-key.signature.agent-ip";
  public static final String FLINK_APP_SINK_KAFKA_MESSAGE_KEY_SIGNATURE_AGENT = "flink.app.sink.kafka.message-key.signature.agent";
  public static final String FLINK_APP_SINK_KAFKA_MESSAGE_KEY_SIGNATURE_IP = "flink.app.sink.kafka.message-key.signature.ip";

  // flink checkpoint
  public static final String CHECKPOINT_DATA_DIR = "flink.app.checkpoint.data-dir";
  public static final String CHECKPOINT_INTERVAL_MS = "flink.app.checkpoint.interval-ms";
  public static final String CHECKPOINT_TIMEOUT_MS = "flink.app.checkpoint.timeout-ms";
  public static final String CHECKPOINT_MIN_PAUSE_BETWEEN_MS = "flink.app.checkpoint.min-pause-between-ms";
  public static final String CHECKPOINT_MAX_CONCURRENT = "flink.app.checkpoint.max-concurrent";
  public static final String TOLERATE_FAILURE_CHECKPOINT_NUMBER = "flink.app.checkpoint.tolerate-failure-number";

  // flink - parallelism
  public static final String DEFAULT_PARALLELISM = "flink.app.parallelism.default";
  public static final String SOURCE_PARALLELISM = "flink.app.parallelism.source";
  public static final String EVENT_PARALLELISM = "flink.app.parallelism.event";
  public static final String SESSION_PARALLELISM = "flink.app.parallelism.session";
  public static final String PRE_AGENT_IP_PARALLELISM = "flink.app.parallelism.pre-agent-ip";
  public static final String AGENT_IP_PARALLELISM = "flink.app.parallelism.agent-ip";
  public static final String AGENT_PARALLELISM = "flink.app.parallelism.agent";
  public static final String IP_PARALLELISM = "flink.app.parallelism.ip";
  public static final String BROADCAST_PARALLELISM = "flink.app.parallelism.broadcast";
  public static final String METRICS_PARALLELISM = "flink.app.parallelism.metrics";
  public static final String SINK_KAFKA_PARALLELISM = "flink.app.parallelism.sink-kafka";
  public static final String METRIICS_COLLECTOR_PARALLELISM = "flink.app.parallelism.metrics-collector";
  public static final String TDQ_NORMALIZER_PARALLELISM = "flink.app.parallelism.tdq-normalizer";
  public static final String METRIICS_COLLECTOR_POST_PARALLELISM = "flink.app.parallelism.metrics-collector-post";
  public static final String METRIICS_COLLECTOR_FINAL_PARALLELISM = "flink.app.parallelism.metrics-collector-final";
  // flink slot share group
  public static final String SOURCE_EVENT_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.source-event";
  public static final String SOURCE_EVENT_LVS_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.source-event-lvs";
  public static final String SOURCE_EVENT_SLC_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.source-event-slc";
  public static final String SOURCE_EVENT_RNO_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.source-event-rno";
  public static final String SESSION_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.session";
  public static final String CROSS_SESSION_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.cross-session";
  public static final String TDQ_NORMALIZER_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.tdq-normalizer";
  public static final String METRICS_COLLECTOR_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.metrics-collector";
  public static final String METRICS_COLLECTOR_POST_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.metrics-collector-post";
  public static final String METRICS_COLLECTOR_FINAL_SLOT_SHARE_GROUP = "flink.app.slot-sharing-group.metrics-collector-final";
  // flink - operator name and uid
  public static final String SOURCE_OPERATOR_NAME_RNO = "flink.app.operator-name.source.rno";
  public static final String SOURCE_OPERATOR_NAME_LVS = "flink.app.operator-name.source.lvs";
  public static final String SOURCE_OPERATOR_NAME_SLC = "flink.app.operator-name.source.slc";
  public static final String SOURCE_UID_RNO = "flink.app.uid.source.rno";
  public static final String SOURCE_UID_LVS = "flink.app.uid.source.lvs";
  public static final String SOURCE_UID_SLC = "flink.app.uid.source.slc";

  // flink - metric name
  public static final String FLINK_APP_METRIC_NAME = "flink.app.metric.watermark-process-progress";
  public static final String SOJOURNER_METRICS_COLLECTOR = "flink.app.slot-sharing-group.metrics-collector";

  // zookeeper
  public static final String ZOOKEEPER_SERVER = "zookeeper.server";
  public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.sessionTimeoutMs";
  public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connectionTimeoutMs";
  public static final String ZOOKEEPER_MAX_RETRIES = "zookeeper.maxRetries";
  public static final String ZOOKEEPER_BASE_SLEEP_TIME_MS = "zookeeper.baseSleepTimeMs";
  public static final String ZOOKEEPER_NAMESPACE = "zookeeper.namespace";

  // rest client
  public static final String REST_BASE_URL = "rest-client.base-url";
  public static final String REST_USERNAME = "rest-client.username";
  public static final String REST_CONFIG_PULL_INTERVAL = "rest-client.config.pull-interval";
  public static final String REST_CONFIG_ENV = "rest-client.config.env";
  public static final String REST_CONFIG_PROFILE = "rest-client.config.profile";

  // ------------------------- batch pipeline common property ---------------------------
  // flink - parallelism
  public static final String SINK_HDFS_PARALLELISM = "flink.app.parallelism.sink.hdfs";

  // flink - operator name and uid
  public static final String SOURCE_OPERATOR_NAME = "flink.app.operator-name.source";
  public static final String SOURCE_UID = "flink.app.uid.source";
  public static final String PASS_THROUGH_OPERATOR_NAME = "flink.app.operator-name.pass-through";
  public static final String PASS_THROUGH_UID = "flink.app.uid.pass-through";
  public static final String TIMESTAMP_EXTRACT_OPERATOR_NAME = "flink.app.operator-name.extract-watermark";
  public static final String TIMESTAMP_EXTRACT_UID = "flink.app.uid.extract-watermark";
  public static final String SESSION_SPLIT_OPERATOR_NAME = "flink.app.operator-name.session-split";
  public static final String SESSION_SPLIT_UID = "flink.app.uid.session-split";
  public static final String ASSIGN_WATERMARK_OPERATOR_NAME = "flink.app.operator-name.assgin-watermark";
  public static final String ASSIGN_WATERMARK_UID = "flink.app.uid.assgin-watermark";
  public static final String SINK_OPERATOR_NAME_EVENT = "flink.app.operator-name.sink.event";
  public static final String SINK_UID_EVENT = "flink.app.uid.sink.event";
  public static final String SINK_OPERATOR_NAME_WATERMARK = "flink.app.operator-name.sink.watermark";
  public static final String SINK_UID_WATERMARK = "flink.app.uid.sink.watermark";
  public static final String SINK_OPERATOR_NAME_SESSION_OPEN = "flink.app.operator-name.sink.session.open";
  public static final String SINK_UID_SESSION_OPEN = "flink.app.uid.sink.session.open";
  public static final String SINK_OPERATOR_NAME_SESSION_SAME_DAY = "flink.app.operator-name.sink.session.same-day";
  public static final String SINK_UID_SESSION_SAME_DAY = "flink.app.uid.sink.session.same-day";
  public static final String SINK_OPERATOR_NAME_SESSION_CROSS_DAY = "flink.app.operator-name.sink.session.cross-day";
  public static final String SINK_UID_SESSION_CROSS_DAY = "flink.app.uid.sink.session.cross-day";
  public static final String SINK_OPERATOR_NAME = "flink.app.operator-name.sink";
  public static final String SINK_UID = "flink.app.uid.sink";

  // flink hdfs sink
  public static final String FLINK_APP_SINK_HDFS_PATH = "flink.app.sink.hdfs.path";
  public static final String FLINK_APP_SINK_HDFS_SAME_DAY_SESSION_PATH = "flink.app.sink.hdfs.path.same-day";
  public static final String FLINK_APP_SINK_HDFS_CROSS_DAY_SESSION_PATH = "flink.app.sink.hdfs.path.cross-day";
  public static final String FLINK_APP_SINK_HDFS_OPEN_SESSION_PATH = "flink.app.sink.hdfs.path.open";
  public static final String FLINK_APP_SINK_HDFS_CLASS = "flink.app.sink.hdfs.class-name";
  public static final String FLINK_APP_SINK_HDFS_WATERMARK_PATH = "flink.app.sink.hdfs.watermark-path";

  // data skew
  public static final String IS_FILTER = "flink.app.data-skew.is-filter";
  public static final String FILTER_GUID_SET = "flink.app.data-skew.guid-set";
  public static final String FILTER_PAGE_ID_SET = "flink.app.data-skew.pageid-set";

  // missing cnt exclude pagefamilies
  public static final String MISSING_CNT_EXCLUDE = "missing-cnt-exclude.u";

}
