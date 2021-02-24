package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.TopicPageIdMapping;
import com.ebay.sojourner.common.util.RestClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class TdqConfigSourceFunction extends RichSourceFunction<PageIdTopicMapping> {

  private RestClient restClient;
  private ObjectMapper objectMapper;
  private final String baseURL;
  private final Long interval;
  private final String env;

  public TdqConfigSourceFunction(String baseURL, Long interval, String env) {
    this.baseURL = baseURL;
    this.interval = interval;
    this.env = env;
  }

  @Override
  public void run(SourceContext<PageIdTopicMapping> ctx) throws Exception {
    while (true) {
      try {
        Response response = restClient.get(
            "/api/custom_topic_config/list/topic_page_ids?env=" + env);
        List<TopicPageIdMapping> configs =
            objectMapper
            .reader()
            .forType(new TypeReference<List<TopicPageIdMapping>>() {})
            .readValue(response.body().string());

        List<PageIdTopicMapping> mappings = new ArrayList<>();
        for (TopicPageIdMapping config : configs) {
          for (Integer pageId : config.getPageIds()) {
            String topic = config.getTopic();
            Optional<PageIdTopicMapping> optional = mappings.stream()
                        .filter(e -> e.getPageId().equals(pageId))
                        .findFirst();
            if (optional.isPresent()) {
              optional.get().getTopics().add(config.getTopic());
            } else {
              PageIdTopicMapping item = new PageIdTopicMapping();
              item.setPageId(pageId);
              item.setTopics(Sets.newHashSet(topic));
              item.setEnv(config.getEnv());
              mappings.add(item);
            }
          }
        }

        for (PageIdTopicMapping mapping : mappings) {
          ctx.collect(mapping);
        }
      } catch (Exception e) {
        log.error("Error when calling rest api");
      }

      Thread.sleep(interval);
    }
  }

  @Override
  public void cancel() {
    log.info("MappingSourceFunction cancelled");
    restClient = null;
    objectMapper = null;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.restClient = new RestClient(baseURL);
    this.objectMapper = new ObjectMapper();
  }
}
