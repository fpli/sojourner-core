package com.ebay.sojourner.ubd.common.metrics;

import com.ebay.sojourner.ubd.common.sharedlib.metrics.SessionDwellMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.*;

import java.util.Collection;
import java.util.List;

public class SessionDwellMetricsTest extends BaseMetricsTest {

    private SessionDwellMetrics sessionDwellMetrics;
    private Pair<JsonNode, List<MetricsTestCase>> pair;

    @BeforeEach
    public void setup() throws Exception {
        sessionDwellMetrics = new SessionDwellMetrics();
        pair = loadTestCases("SessionDwellMetricsTest.yaml");
    }


    @TestFactory
    public Collection<DynamicTest> dynamicTests() throws Exception {
        return generateDynamicTests(pair.getRight(), pair.getLeft(), sessionDwellMetrics);
    }

}