# Realtime Processor

## Merge Rheos job mainArgs
We log these mainArgs for validation or rollback purpose. After new configs are active, we can remove the logs here.

### before merge

[sojourner-realtime-rno/sojourner-realtime-pipeline](https://rhs-portal.vip.ebay.com/flink/job/sojourner-realtime-rno/sojourner-realtime-pipeline)

```
--flink.app.parallelism.pre-agent-ip 150 
--flink.app.parallelism.agent 150 
--flink.app.parallelism.source 200 
--flink.app.parallelism.session 1000 
--flink.app.parallelism.event 200 
--flink.app.parallelism.ip 150 
--flink.app.parallelism.agent-ip 150 
--flink.app.parallelism.broadcast 510 
--flink.app.parallelism.metrics 510 
--flink.app.parallelism.default 150 
--flink.app.sink.kafka.subject.event behavior.sojourner.sojevent.schema 
--flink.app.message-filter.max-message-bytes 204800 
--flink.app.debug-mode false 
--kafka.producer.delivery-timeout-ms 180000
```

[sojourner-realtime-lvs/sojourner-realtime-pipeline](https://rhs-portal.vip.ebay.com/flink/job/sojourner-realtime-lvs/sojourner-realtime-pipeline)

```
--flink.app.parallelism.pre-agent-ip 150 
--flink.app.parallelism.agent 150 
--flink.app.parallelism.source 200 
--flink.app.parallelism.session 1000 
--flink.app.parallelism.event 200 
--flink.app.parallelism.ip 150 
--flink.app.parallelism.agent-ip 150 
--flink.app.parallelism.broadcast 510 
--flink.app.parallelism.metrics 510 
--flink.app.parallelism.default 150 
--flink.app.sink.kafka.subject.event behavior.sojourner.sojevent.schema 
--flink.app.message-filter.max-message-bytes 204800 
--flink.app.debug-mode false 
--flink.app.checkpoint.interval-ms 300000 
--kafka.producer.delivery-timeout-ms 180000
```

### After merge

All mainArgs are merged to `src/main/resources/application.yml`, Rheos job mainArgs are not used anymore.

new config values are equivalent to:

```
--flink.app.parallelism.pre-agent-ip 150
--flink.app.parallelism.agent 150
--flink.app.parallelism.source 200
--flink.app.parallelism.session 1000
--flink.app.parallelism.event 200
--flink.app.parallelism.ip 150
--flink.app.parallelism.agent-ip 150
--flink.app.parallelism.broadcast 510
--flink.app.parallelism.metrics 510
--flink.app.parallelism.default 150
--flink.app.sink.kafka.subject.event behavior.sojourner.sojevent.schema
--flink.app.message-filter.max-message-bytes 204800
--flink.app.debug-mode false
--flink.app.checkpoint.interval-ms 300000
--kafka.producer.delivery-timeout-ms 180000
```