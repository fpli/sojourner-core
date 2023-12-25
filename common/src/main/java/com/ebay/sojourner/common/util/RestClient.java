package com.ebay.sojourner.common.util;

import static okhttp3.Credentials.basic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;

@Slf4j
public class RestClient {

  public static final String X_AUTH_USERNAME_HEADER = "X-Auth-Username";
  public static final String X_AUTH_TOKEN_HEADER = "X-Auth-Token";

  private final String baseURL;
  private final OkHttpClient okHttpClient;

  public RestClient(String baseURL) {
    this.baseURL = baseURL;
    okHttpClient = new OkHttpClient.Builder().build();
  }

  public Response get(String api) throws IOException {
    String url = baseURL + api;

    Request request = new Builder()
        .url(url)
        .addHeader(X_AUTH_USERNAME_HEADER, "soj-flink-app")
        .addHeader(X_AUTH_TOKEN_HEADER,
                   basic("sojourner", "sojourner", StandardCharsets.UTF_8))
        .build();

    return okHttpClient.newCall(request).execute();
  }

}
