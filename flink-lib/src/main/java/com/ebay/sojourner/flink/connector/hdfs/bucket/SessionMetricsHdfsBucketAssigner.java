package com.ebay.sojourner.flink.connector.hdfs.bucket;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

@Slf4j
public class SessionMetricsHdfsBucketAssigner implements BucketAssigner<SessionMetrics, String> {

    @Override
    public String getBucketId(SessionMetrics sessionMetrics, Context context) {
        StringBuilder sb = new StringBuilder();

        long sessionEndTimestamp = System.currentTimeMillis();
        long sessionStartDt = System.currentTimeMillis();

        try {
            sessionEndTimestamp = SojDateTimeUtils.toEpochMilli(sessionMetrics.getAbsEndTimestamp());
            sessionStartDt = SojDateTimeUtils.toEpochMilli(sessionMetrics.getSessionStartDt());
        } catch (Exception e) {
            log.warn("session end time is null: " + sessionEndTimestamp);
            log.warn("session start time is null: " + sessionStartDt);
        }

        String sessionEndDtStr = SojDateTimeUtils.toDateString(sessionEndTimestamp);
        String sessionStartDtStr = SojDateTimeUtils.toDateString(sessionStartDt);
        String sessionStartHrStr = SojDateTimeUtils.toHrString(sessionStartDt);

        // for session category
        if (sessionMetrics.getIsOpen()) {
            sb.append("category=open");
        } else if (sessionStartDtStr.equals(sessionEndDtStr)) {
            sb.append("category=sameday");
        } else {
            sb.append("category=crossday");
        }

        // for dt and hr
        sb.append("/dt=")
          .append(sessionStartDtStr);

        sb.append("/hr=")
          .append(sessionStartHrStr);

        return sb.toString();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

}
