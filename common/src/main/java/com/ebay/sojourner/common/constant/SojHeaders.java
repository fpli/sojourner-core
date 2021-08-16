package com.ebay.sojourner.common.constant;

public class SojHeaders {
  public static final String SCHEMA_VERSION = "schemaVersion";

  //Entry Page flag
  public static final String EP="EP";

  // pathfinder record event create timestamp
  public static final String PATHFINDER_CREATE_TIMESTAMP = "pct";

  // pathfinder record event sent timestamp
  public static final String PATHFINDER_SENT_TIMESTAMP = "pst";

  // pathfinder kafka producer record event timestamp
  public static final String PATHFINDER_PRODUCER_TIMESTAMP = "ppt";

  // totalv2 kafka producer record event timestamp
  public static final String REALTIME_PRODUCER_TIMESTAMP = "rpt";

  // distributor ingest time
  public static final String DISTRIBUTOR_INGEST_TIMESTAMP = "dit";

}
