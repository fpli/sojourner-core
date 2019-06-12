package com.ebay.sojourner.ubd.common.sharedlib.connectors;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CouchBaseConnector {
    private static CouchBaseConnector couchBaseConnector;
    private static Cluster couchBaseCluster = null;
    private static final String serverName = "127.0.0.1";
    private static final String USER_NAME = "Administrator";
    private static final String USER_PASS = "111111";
    private static final String BUCKET_NAME = "botsignature";
    private static Bucket bucket = null;

    private CouchBaseConnector() {
        couchBaseCluster = CouchbaseCluster.create(serverName);
        couchBaseCluster.authenticate(USER_NAME, USER_PASS);
        bucket = couchBaseCluster.openBucket(BUCKET_NAME);
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);
    }

    public static CouchBaseConnector getInstance() {
        if (couchBaseConnector == null) {
            synchronized (CouchBaseConnector.class) {
                if (couchBaseConnector == null) {
                    couchBaseConnector = new CouchBaseConnector();

                }
            }
        }
        return couchBaseConnector;
    }

    public void insUpsert(JsonObject jsonObject, String id) {

        bucket.upsert(JsonDocument.create(id, jsonObject));

    }

    public Set<Integer> scanSignature(String inColumnName, String inColumnValue, String outColumnName) {
        N1qlQueryResult result = bucket.query(
                N1qlQuery.parameterized("SELECT " + outColumnName + " FROM " + BUCKET_NAME + " WHERE " + inColumnName + " =\"$1\"",
                        JsonArray.from(inColumnValue)));

        for (N1qlQueryRow row : result) {
            JsonArray jsonArray = (JsonArray) row.value().get(outColumnName);
            List<Object> botFlagList = (List<Object>) jsonArray.toList();
            Set<Integer> botFlagSet = new HashSet<Integer>(botFlagList.size());
            for (Object o : botFlagList) {
                botFlagSet.add((Integer) o);
            }
            return botFlagSet;
        }
        return null;
    }

    public static void close() {
        bucket.close();
        couchBaseCluster.disconnect();
    }

}
