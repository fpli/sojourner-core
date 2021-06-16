package com.ebay.sojourner.distributor.route;

import java.io.Serializable;
import java.util.Set;

public interface Router<T> extends Serializable {
  Set<String> target(T t);
}
