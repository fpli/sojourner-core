package com.ebay.sojourner.flink.connector.kafka.rheos;

public class RheosStreamConfigException extends RuntimeException {

  public RheosStreamConfigException() {
  }

  public RheosStreamConfigException(String message) {
    super(message);
  }

  public RheosStreamConfigException(String message, Throwable cause) {
    super(message, cause);
  }

  public RheosStreamConfigException(Throwable cause) {
    super(cause);
  }

  public RheosStreamConfigException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
