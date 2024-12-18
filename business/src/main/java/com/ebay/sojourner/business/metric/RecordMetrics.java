package com.ebay.sojourner.business.metric;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RecordMetrics<Source, Target> implements Aggregator<Source, Target> {

  protected Set<FieldMetrics<Source, Target>> fieldMetrics
      = new CopyOnWriteArraySet<>();

  /**
   * Initialize the field metrics for being used in aggregator operations.
   */
  public abstract void initFieldMetrics();

  public void init() throws Exception {
    for (FieldMetrics<Source, Target> metrics : fieldMetrics) {
      metrics.init();
    }
  }

  public void start(Target target) throws Exception {
    for (FieldMetrics<Source, Target> metrics : fieldMetrics) {
      metrics.start(target);
    }
  }

  public void feed(Source source, Target target) throws Exception {
    for (FieldMetrics<Source, Target> metrics : fieldMetrics) {
      try {
        metrics.feed(source, target);
      }catch(Exception e){
        //        log.error(" session metric feed issue :"+e.getMessage());
      }
    }
  }

  public void end(Target target) throws Exception {
    for (FieldMetrics<Source, Target> metrics : fieldMetrics) {
      metrics.end(target);
    }
  }

  public void addFieldMetrics(FieldMetrics<Source, Target> metrics) {
    // TODO: this line is always true sine FieldMetrics does not override equals()
    if (!fieldMetrics.contains(metrics)) {
      fieldMetrics.add(metrics);
    } else {
      throw new RuntimeException("Duplicate Metrics!");
    }
  }
}
