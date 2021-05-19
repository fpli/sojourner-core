package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.TopicPageIdMapping;
import com.ebay.sojourner.common.util.RestClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

@Slf4j
public class CustomTopicConfigSourceFunction extends RichSourceFunction<PageIdTopicMapping>
    implements CheckpointedFunction {

  private RestClient restClient;
  private ObjectMapper objectMapper;
  private final String baseURL;
  private final Long interval;
  private final String profile;
  private final ListStateDescriptor<CustomTopicConfig> stateDescriptor;
  private ListState<CustomTopicConfig> state;
  private Set<Integer> latestPageIds = new HashSet<>();

  public CustomTopicConfigSourceFunction(String baseURL, Long interval, String profile,
                                         ListStateDescriptor<CustomTopicConfig> stateDescriptor) {
    this.baseURL = baseURL;
    this.interval = interval;
    this.profile = profile;
    this.stateDescriptor = stateDescriptor;
  }

  @Override
  public void run(SourceContext<PageIdTopicMapping> ctx) throws Exception {
    final Object lock = ctx.getCheckpointLock();
    while (true) {
      try {
        Response response = restClient.get(
            "/api/custom_topic_config/list/topic_page_ids?profile=" + profile);
        List<TopicPageIdMapping> configs = objectMapper
            .reader()
            .forType(new TypeReference<List<TopicPageIdMapping>>() {})
            .readValue(response.body().string());

        // do nothing if there is no config returned
        if (CollectionUtils.isNotEmpty(configs)) {
          List<PageIdTopicMapping> mappings = revertMappings(configs);

          // state update is atomic
          synchronized (lock) {
            // clone a new temp pageIds from latestPageIds
            final Set<Integer> tmpPageIds = new HashSet<>(latestPageIds);

            // reset latestPageIds to store latest pageids
            latestPageIds.clear();
            for (PageIdTopicMapping mapping : mappings) {
              ctx.collect(mapping);
              tmpPageIds.remove(mapping.getPageId());
              latestPageIds.add(mapping.getPageId());
            }

            // to remove topic/pageid mapping
            for (Integer p : tmpPageIds) {
              ctx.collect(new PageIdTopicMapping(p, null, null));
            }
          }
        }
      } catch (Exception e) {
        log.error("Error when calling rest api", e);
      }

      Thread.sleep(interval);
    }
  }

  private List<PageIdTopicMapping> revertMappings(List<TopicPageIdMapping> configs) {
    List<PageIdTopicMapping> mappings = new ArrayList<>();
    for (TopicPageIdMapping config : configs) {
      for (Integer pageId : config.getPageIds()) {
        String topic = config.getTopic();
        Optional<PageIdTopicMapping> optional = mappings.stream()
                                                        .filter(e -> e.getPageId()
                                                                      .equals(pageId))
                                                        .findFirst();
        if (optional.isPresent()) {
          optional.get().getTopics().add(config.getTopic());
        } else {
          PageIdTopicMapping item = new PageIdTopicMapping();
          item.setPageId(pageId);
          item.setTopics(Sets.newHashSet(topic));
          item.setProfile(config.getProfile());
          mappings.add(item);
        }
      }
    }
    return mappings;
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


  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    state.clear();
    CustomTopicConfig customTopicConfig = new CustomTopicConfig();
    for (Integer p : latestPageIds) {
      customTopicConfig.getPageIds().add(p);
    }
    state.add(customTopicConfig);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    state = context.getOperatorStateStore()
                   .getListState(stateDescriptor);
    // restore pageids from state
    for (CustomTopicConfig config : state.get()) {
      latestPageIds = new HashSet<>(config.getPageIds());
    }
  }
}
