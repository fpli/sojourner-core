package com.ebay.sojourner.ubd.common.parsertest;

import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.sharedlib.parser.CiidParser;
import com.ebay.sojourner.ubd.common.sharelib.Constants;
import com.ebay.sojourner.ubd.common.sharelib.LoadRawEventAndExpect;
import com.ebay.sojourner.ubd.common.sharelib.VaildateResult;
import com.ebay.sojourner.ubd.common.util.TypeTransUtil;
import com.ebay.sojourner.ubd.common.util.YamlUtil;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CiidParserTest {
    private static final Logger logger = Logger.getLogger(CiidParserTest.class);

    private static UbiEvent ubiEvent = null;
    private static String parser = null;
    private static String caseItem = null;
    private static CiidParser ciidParser = null;
    private static HashMap<String, Object> map = null;

    @BeforeClass
    public static void initParser() {
        parser = Constants.CIID;
        map = YamlUtil.getInstance().loadFileMap(Constants.FILEPATH);
    }

    @Test
    public void testCiidParser1() {
        ciidParser = new CiidParser();
        ubiEvent = new UbiEvent();
        caseItem = Constants.CASE1;

        HashMap<RawEvent, Object> rawEventAndExpectResult = LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
        for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
            ciidParser.parse(entry.getKey(), ubiEvent);
            System.out.println(VaildateResult.vaildateString(entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getCurrentImprId())));
        }
    }

    @Test
    public void testCiidParser2() {
        ciidParser = new CiidParser();
        ubiEvent = new UbiEvent();
        caseItem = Constants.CASE2;

        HashMap<RawEvent, Object> rawEventAndExpectResult = LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
        for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
            ciidParser.parse(entry.getKey(), ubiEvent);
            System.out.println(VaildateResult.vaildateString(entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getCurrentImprId())));
        }
    }


    @Test
    public void testCiidParser3() {
        ciidParser = new CiidParser();
        ubiEvent = new UbiEvent();
        caseItem = Constants.CASE3;

        HashMap<RawEvent, Object> rawEventAndExpectResult = LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
        for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
            ciidParser.parse(entry.getKey(), ubiEvent);
            System.out.println(VaildateResult.vaildateString(entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getCurrentImprId())));
        }
    }

    @Test
    public void testCiidParser4() {
        ciidParser = new CiidParser();
        ubiEvent = new UbiEvent();
        caseItem = Constants.CASE4;

        HashMap<RawEvent, Object> rawEventAndExpectResult = LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
        for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
            ciidParser.parse(entry.getKey(), ubiEvent);
            System.out.println(VaildateResult.vaildateString(entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getCurrentImprId())));
        }
    }
}
