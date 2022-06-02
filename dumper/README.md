# Dumper

## Merge Rheos job mainArgs
We log these mainArgs for validation or rollback purpose. After new configs are active, we can remove the logs here.

### Before merge - sojourner-dumper-lvs

As of this writing, dumper jobs are running in sojourner-dumper-lvs.

[sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-event-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-event-nonbot)

```
--profile lvsdrnonbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.from-timestamp 1651670280000 
--flink.app.source.dc lvs
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-event-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-event-bot)

```
--profile lvsdrbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.dc lvs 
--flink.app.source.from-timestamp 1651669980000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-session-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-session-nonbot)

```
--profile lvsdrnonbotsession 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.source.from-timestamp 1651669500000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-session-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-lvs-session-bot)

```
--profile lvsdrbotsession 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.source.from-timestamp 1651669200000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-event-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-event-nonbot)

```
--profile rnodrnonbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 250 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.dc lvs 
--flink.app.source.from-timestamp 1651668300000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-event-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-event-bot)

```
--profile rnodrbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.dc lvs 
--flink.app.source.from-timestamp 1651668000000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-session-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-session-nonbot)

```
--profile rnodrnonbotsession 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.source.from-timestamp 1651667400000
```

[sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-session-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-lvs/sojourner-hdfs-dumper-rno-session-bot)

```
--profile rnodrbotsession 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.source.from-timestamp 1651665600000
```

### Before merge - sojourner-dumper-rno

[sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-event-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-event-nonbot)

```
--profile lvsdrnonbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-event-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-event-bot)

```
--profile lvsdrbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-session-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-session-nonbot)

```
--profile lvsdrnonbotsession
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-session-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-lvs-session-bot)

```
--profile lvsdrbotsession
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-rno-event-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-rno-event-nonbot)

```
--profile rnodrnonbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 250 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.from-timestamp 1650536474000
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-rno-event-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-rno-event-bot)

```
--profile rnodrbotevent 
--kafka.consumer.auto-offset-reset earliest 
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.from-timestamp 1650536474000
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-rno-session-nonbot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-rno-session-nonbot)

```
--profile rnodrnonbotsession
```

[sojourner-dumper-rno/sojourner-hdfs-dumper-rno-session-bot](https://rhs-portal.vip.ebay.com/flink/job/sojourner-dumper-rno/sojourner-hdfs-dumper-rno-session-bot)

```
--profile rnodrbotsession
```

### After merge

All mainArgs are merged to `src/main/resources/application*.yml`, Rheos job mainArgs are not used anymore.

new config values for event dumpers are equivalent to:

```
--flink.app.parallelism.source 200 
--flink.app.parallelism.sink.hdfs 500 
--flink.app.source.from-timestamp 0
--kafka.consumer.auto-offset-reset latest
```

config values of session dumpers are not changed.

Notes about tech details:

* Start modes including latest, earliest, group offsets (default if not set) does not affect where partitions are read 
  from when the consumer is restored from a checkpoint or savepoint. When the consumer is restored from a checkpoint or 
  savepoint, only the offsets in the restored state will be used.
* If offsets in the restored state do not exist when job is being restored (e.g. offsets are removed due to retention
  policy), the earliest offsets will be used.
* `auto.offset.reset` is only used when start mode is set to group offsets (i.e. the default one). Since we never use
  group offsets to start jobs, `auto.offset.rest` will never be used.
* `from-timestamp` is somehow misleading. A more appropriate name may be `starting-offsets`.

For more details:

* https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/#starting-offset
* https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/connectors/kafka.html#kafka-consumers-start-position-configuration
* https://github.com/apache/flink/blob/release-1.11/flink-connectors/flink-connector-kafka-base/src/main/java/org/apache/flink/streaming/connectors/kafka/FlinkKafkaConsumerBase.java#L577-L582
