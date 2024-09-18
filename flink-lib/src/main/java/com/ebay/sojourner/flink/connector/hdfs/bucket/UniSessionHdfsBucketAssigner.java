package com.ebay.sojourner.flink.connector.hdfs.bucket;

import com.ebay.sojourner.common.model.UniSession;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

@Slf4j
public class UniSessionHdfsBucketAssigner implements BucketAssigner<UniSession, String> {

    @Override
    public String getBucketId(UniSession uniSession, Context context) {
        StringBuilder sb = new StringBuilder();

        long sessionEndTimestamp = System.currentTimeMillis();
        long sessionStartDt = System.currentTimeMillis();

        Long absEndTimestamp = uniSession.getAbsEndTimestamp();
        if (absEndTimestamp != null){
            sessionEndTimestamp = absEndTimestamp;
        }

        Long sessionSessionStartDt = uniSession.getSessionStartDt();
        if (sessionSessionStartDt != null) {
            sessionStartDt = sessionSessionStartDt;
        }

        String sessionEndDtStr = SojDateTimeUtils.toDateString(sessionEndTimestamp);
        String sessionStartDtStr = SojDateTimeUtils.toDateString(sessionStartDt);
        String sessionStartHrStr = SojDateTimeUtils.toHrString(sessionStartDt);

        // for session category
        if (uniSession.getIsOpen()) {
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
