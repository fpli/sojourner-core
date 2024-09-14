package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.SojEvent;
import org.apache.flink.metrics.Counter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;


public class GuidXUidFlatMapTest {

    private SojEvent sojEvent;

    private GuidXUidFlatMap uidFilterFunction;

    private Counter collectEventCounter = mock(Counter.class);

    private Counter dropEventCounter = mock(Counter.class);

    private Counter invalidUserIdEventCounter = mock(Counter.class);

    private Counter invalidBuEventCounter = mock(Counter.class);

    private Counter invalidUserCounter = mock(Counter.class);

    @BeforeEach
    public void setUp() {
        uidFilterFunction = new GuidXUidFlatMap();
        //uidFilterFunction
        doNothing().when(collectEventCounter).inc(); // can be removed
        uidFilterFunction.setCollectEventCounter(collectEventCounter);
        doNothing().when(dropEventCounter).inc();
        uidFilterFunction.setDropEventCounter(dropEventCounter);
        doNothing().when(invalidUserIdEventCounter).inc();
        uidFilterFunction.setInvalidUserIdEventCounter(invalidUserIdEventCounter);
        doNothing().when(invalidBuEventCounter).inc();
        uidFilterFunction.setInvalidBuEventCounter(invalidBuEventCounter);
        doNothing().when(invalidUserCounter).inc();
        uidFilterFunction.setInvalidUserCounter(invalidUserCounter);
        sojEvent = new SojEvent();
        sojEvent.setPageId(3084);
    }

    /**
     * guid is null
     * @throws Exception
     */
    @Test
    public void testGuidIsNull() throws Exception {
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    /**
     * guid is invalid
     * @throws Exception
     */
    @Test
    public void testGuidIsInvalid() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589HH");
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    /**
     * iframe is null
     * @throws Exception
     */
    @Test
    public void testIframeIsNull() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);

        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isTrue();
    }

    // guid_uid_quality_validation_report_monitoring

    /**
     * iframe is true
     * @throws Exception
     */
    @Test
    public void test_iframe_is_true() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);
        sojEvent.setIframe(true);
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    /**
     * rdt is 1
     * @throws Exception
     */
    @Test
    public void test_rdt_is_one() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(1);
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    /**
     * userId is 0, but bu is valid
     * @throws Exception
     */
    @Test
    public void test_userid_is_zero_but_bu_is_valid() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);
        sojEvent.setUserId("0");
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isTrue();
    }

    /**
     * userId is -1, but bu is valid
     * @throws Exception
     */
    @Test
    public void test_userid_is_negative_one_but_bu_is_valid() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);
        sojEvent.setUserId("-1");
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "2426169115");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isTrue();
    }

    /**
     * userId is null, but bu is null
     * @throws Exception
     */
    @Test
    public void f7() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);

        Map<String, String> appPayload = new HashMap<>();

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    /**
     * userId is valid, but bu is valid
     * @throws Exception
     */
    @Test
    public void f8() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);
        sojEvent.setUserId("81773989");
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "1808112384");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isTrue();
    }

    /**
     * userId is invalid, but bu is invalid
     * @throws Exception
     */
    @Test
    public void f9() throws Exception {
        sojEvent.setGuid("d64c8cd418c0a5485853b5bafffb9589");
        sojEvent.setRdt(0);
        sojEvent.setUserId("81773989323");
        Map<String, String> appPayload = new HashMap<>();
        appPayload.put("bu", "1808112384223");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void f10() throws Exception {
        sojEvent.setGuid("007d94191898b89efa6ed8d001203a7d");
        sojEvent.setRdt(0);
        sojEvent.setUserId("1600766613");
        sojEvent.setIframe(false);
        Map<String, String> appPayload = new HashMap<>();
        //appPayload.put("bu", "1808112384223");

        sojEvent.setApplicationPayload(appPayload);

        boolean result = uidFilterFunction.filter(sojEvent);
        Assertions.assertThat(result).isTrue();
    }
}
