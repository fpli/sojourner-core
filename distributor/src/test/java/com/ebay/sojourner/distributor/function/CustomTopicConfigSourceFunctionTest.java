package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.CustomTopicConfig;
import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.TopicPageIdMapping;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomTopicConfigSourceFunctionTest {

  CustomTopicConfigSourceFunction customTopicConfigSourceFunction;

  @BeforeEach
  void setUp() {
    customTopicConfigSourceFunction = new CustomTopicConfigSourceFunction(
        "localhost", 10L, "test",
        new ListStateDescriptor<>("customTopicConfigListState", CustomTopicConfig.class)
    );
  }

  @Test
  void test_revertMappings_OK() throws Exception {
    List<TopicPageIdMapping> input = new ArrayList<>();
    input.add(new TopicPageIdMapping("topic1", Sets.newHashSet(1,2,3), "test"));
    input.add(new TopicPageIdMapping("topic2", Sets.newHashSet(1,2,3), "test"));
    input.add(new TopicPageIdMapping("topic3", Sets.newHashSet(4), "test"));

    Method revertMappingsMethod = CustomTopicConfigSourceFunction.class
        .getDeclaredMethod("revertMappings", List.class);
    revertMappingsMethod.setAccessible(true);

    List<PageIdTopicMapping> results = (List<PageIdTopicMapping>) revertMappingsMethod.invoke(customTopicConfigSourceFunction, input);

    Assertions.assertThat(results).hasSize(4);
    results.sort(Comparator.comparing(PageIdTopicMapping::getPageId));

    Assertions.assertThat(results.get(0).getPageId()).isEqualTo(1);
    Assertions.assertThat(results.get(0).getTopics()).contains("topic1", "topic2");
    Assertions.assertThat(results.get(1).getPageId()).isEqualTo(2);
    Assertions.assertThat(results.get(1).getTopics()).contains("topic1", "topic2");
    Assertions.assertThat(results.get(2).getPageId()).isEqualTo(3);
    Assertions.assertThat(results.get(2).getTopics()).contains("topic1", "topic2");
    Assertions.assertThat(results.get(3).getPageId()).isEqualTo(4);
    Assertions.assertThat(results.get(3).getTopics()).contains("topic3");
  }
}