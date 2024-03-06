package com.ebay.sojourner.common.env;

public class ConfigNotFoundException extends RuntimeException {

  public ConfigNotFoundException() {
    super();
  }

  public ConfigNotFoundException(String message) {
    super(message);
  }

  public ConfigNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigNotFoundException(Throwable cause) {
    super(cause);
  }

  protected ConfigNotFoundException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
