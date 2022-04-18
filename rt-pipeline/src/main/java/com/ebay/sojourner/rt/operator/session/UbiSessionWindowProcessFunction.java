package com.ebay.sojourner.rt.operator.session;

import com.ebay.sojourner.business.detector.SessionEndBotDetector;
import com.ebay.sojourner.business.metric.SessionMetrics;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiSession;
import java.util.Set;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UbiSessionWindowProcessFunction
    extends ProcessWindowFunction<SessionAccumulator, UbiSession, Tuple, TimeWindow> {

  private static final ValueStateDescriptor<Long> lastTimestampStateDescriptor =
      new ValueStateDescriptor("lastTimestamp", LongSerializer.INSTANCE);

  private SessionEndBotDetector sessionEndBotDetector;

  private void outputSession(UbiSession ubiSessionTmp,
                             Collector<UbiSession> out, boolean isOpen) {
    UbiSession ubiSession = new UbiSession();
    ubiSession.setGuid(ubiSessionTmp.getGuid());
    ubiSession.setAgentString(ubiSessionTmp.getAgentString());
    ubiSession.setSessionId(ubiSessionTmp.getSessionId());
    ubiSession.setSessionSkey(ubiSessionTmp.getSessionSkey());
    ubiSession.setIp(ubiSessionTmp.getIp());
    ubiSession.setUserAgent(ubiSessionTmp.getUserAgent());
    ubiSession.setExInternalIp(ubiSessionTmp.getExInternalIp());
    ubiSession.setSojDataDt(ubiSessionTmp.getSojDataDt());
    ubiSession.setSessionStartDt(ubiSessionTmp.getSessionStartDt());
    ubiSession.setAgentCnt(ubiSessionTmp.getAgentCnt());
    ubiSession.setStartTimestamp(ubiSessionTmp.getStartTimestamp());
    ubiSession.setEndTimestamp(ubiSessionTmp.getEndTimestamp());
    ubiSession.setAbsStartTimestamp(ubiSessionTmp.getAbsStartTimestamp());
    ubiSession.setAbsEndTimestamp(ubiSessionTmp.getAbsEndTimestamp());
    ubiSession.setClientIp(ubiSessionTmp.getClientIp());
    ubiSession.setInternalIp(ubiSessionTmp.getInternalIp());
    ubiSession.setSingleClickSessionFlag(ubiSessionTmp.getSingleClickSessionFlag());
    ubiSession.setBotFlagList(ubiSessionTmp.getBotFlagList());
    ubiSession.setNonIframeRdtEventCnt(ubiSessionTmp.getNonIframeRdtEventCnt());
    ubiSession.setSessionReferrer(ubiSessionTmp.getSessionReferrer());
    ubiSession.setBotFlag(ubiSessionTmp.getBotFlag());
    ubiSession.setVersion(ubiSessionTmp.getVersion());
    ubiSession.setFirstUserId(ubiSessionTmp.getFirstUserId());
    ubiSession.setSiteFlags(ubiSessionTmp.getSiteFlags());
    ubiSession.setAttrFlags(ubiSessionTmp.getAttrFlags());
    ubiSession.setBotFlags(ubiSessionTmp.getBotFlags());
    ubiSession.setFindingFlags(ubiSessionTmp.getFindingFlags());
    ubiSession.setPageId(ubiSessionTmp.getPageId());
    ubiSession.setStartPageId(ubiSessionTmp.getStartPageId());
    ubiSession.setEndPageId(ubiSessionTmp.getEndPageId());
    ubiSession.setDurationSec(ubiSessionTmp.getDurationSec());
    ubiSession.setEventCnt(ubiSessionTmp.getEventCnt());
    ubiSession.setAbsEventCnt(ubiSessionTmp.getAbsEventCnt());
    ubiSession.setViCoreCnt(ubiSessionTmp.getViCoreCnt());
    ubiSession.setBidCoreCnt(ubiSessionTmp.getBidCoreCnt());
    ubiSession.setBinCoreCnt(ubiSessionTmp.getBinCoreCnt());
    ubiSession.setWatchCoreCnt(ubiSessionTmp.getWatchCoreCnt());
    ubiSession.setTrafficSrcId(ubiSessionTmp.getTrafficSrcId());
    ubiSession.setAbsDuration(ubiSessionTmp.getAbsDuration());
    ubiSession.setCobrand(ubiSessionTmp.getCobrand());
    ubiSession.setFirstAppId(ubiSessionTmp.getFirstAppId());
    ubiSession.setFirstSiteId(ubiSessionTmp.getFirstSiteId());
    ubiSession.setFirstCguid(ubiSessionTmp.getFirstCguid());
    ubiSession.setFirstMappedUserId(ubiSessionTmp.getFirstMappedUserId());
    ubiSession.setHomepageCnt(ubiSessionTmp.getHomepageCnt());
    ubiSession.setGr1Cnt(ubiSessionTmp.getGr1Cnt());
    ubiSession.setGrCnt(ubiSessionTmp.getGrCnt());
    ubiSession.setMyebayCnt(ubiSessionTmp.getMyebayCnt());
    ubiSession.setSigninPageCnt(ubiSessionTmp.getSigninPageCnt());
    ubiSession.setAsqCnt(ubiSessionTmp.getAsqCnt());
    ubiSession.setAtcCnt(ubiSessionTmp.getAtcCnt());
    ubiSession.setAtlCnt(ubiSessionTmp.getAtlCnt());
    ubiSession.setBoCnt(ubiSessionTmp.getBoCnt());
    ubiSession.setSrpCnt(ubiSessionTmp.getSrpCnt());
    ubiSession.setServEventCnt(ubiSessionTmp.getServEventCnt());
    ubiSession.setSearchViewPageCnt(ubiSessionTmp.getSearchViewPageCnt());
    ubiSession.setCity(ubiSessionTmp.getCity());
    ubiSession.setRegion(ubiSessionTmp.getRegion());
    ubiSession.setCountry(ubiSessionTmp.getCountry());
    ubiSession.setContinent(ubiSessionTmp.getContinent());
    ubiSession.setBrowserFamily(ubiSessionTmp.getBrowserFamily());
    ubiSession.setBrowserVersion(ubiSessionTmp.getBrowserVersion());
    ubiSession.setDeviceFamily(ubiSessionTmp.getDeviceFamily());
    ubiSession.setDeviceClass(ubiSessionTmp.getDeviceClass());
    ubiSession.setOsFamily(ubiSessionTmp.getOsFamily());
    ubiSession.setOsVersion(ubiSessionTmp.getOsVersion());
    ubiSession.setStartResourceId(ubiSessionTmp.getStartResourceId());
    ubiSession.setEndResourceId(ubiSessionTmp.getEndResourceId());
    ubiSession.setIsReturningVisitor(ubiSessionTmp.isReturningVisitor());
    ubiSession.setLineSpeed(ubiSessionTmp.getLineSpeed());
    ubiSession.setPulsarEventCnt(ubiSessionTmp.getPulsarEventCnt());
    ubiSession.setSessionEndDt(ubiSessionTmp.getSessionEndDt());
    ubiSession.setStreamId(ubiSessionTmp.getStreamId());
    ubiSession.setBuserId(ubiSessionTmp.getBuserId());
    ubiSession.setValidPageCnt(ubiSessionTmp.getValidPageCnt());
    ubiSession.setOpenEmit(isOpen);
    out.collect(ubiSession);
  }

  @Override
  public void process(
      Tuple tuple,
      Context context,
      Iterable<SessionAccumulator> elements,
      Collector<UbiSession> out)
      throws Exception {
    SessionAccumulator sessionAccumulator = elements.iterator().next();
    if (context.currentWatermark() >= context.window().maxTimestamp()) {
      endSessionEvent(sessionAccumulator);
      Set<Integer> botFlagList = sessionEndBotDetector
          .getBotFlagList(sessionAccumulator.getUbiSession());
      sessionAccumulator.getUbiSession().getBotFlagList().addAll(botFlagList);
      outputSession(sessionAccumulator.getUbiSession(), out, false);
    } else {
      endSessionEvent(sessionAccumulator);
      Set<Integer> botFlagList = sessionEndBotDetector
          .getBotFlagList(sessionAccumulator.getUbiSession());
      sessionAccumulator.getUbiSession().getBotFlagList().addAll(botFlagList);
      outputSession(sessionAccumulator.getUbiSession(), out, true);
    }
  }

  private void endSessionEvent(SessionAccumulator sessionAccumulator) throws Exception {
    SessionMetrics.getInstance().end(sessionAccumulator);
  }

  @Override
  public void open(Configuration conf) throws Exception {
    super.open(conf);
    sessionEndBotDetector = SessionEndBotDetector.getInstance();
  }

  @Override
  public void clear(Context context) throws Exception {
    context.globalState().getState(lastTimestampStateDescriptor).clear();
  }
}
