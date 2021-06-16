package com.ebay.sojourner.distributor.route;

public interface RouteRule<T> {
  boolean match(T t);
}
