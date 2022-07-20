package com.ebay.sojourner.common.model;

import com.ebay.sojourner.common.util.SessionCoreHelper;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

@Data
@Slf4j
public class AgentIpAttribute implements Attribute<SessionCore>, Serializable {

  public static final Set<Integer> REF_211 = new HashSet<>(
      Arrays.asList(201, 203, 204, 205, 206, 207, 208, 212));
  // Shuffle Key
  private Integer clientIp;
  private AgentHash agent;
  private long timestamp;
  private Set<Integer> botFlagList = new LinkedHashSet<>();

  // For Bot5
  private int scsCountForBot5 = 0;

  // For Bot6
  private int scsCountForBot6 = 0;
  private Set<Integer> ipSet = new HashSet<>();

  // For Bot7
  private int scsCountForBot7 = 0;


  //For Bot8
  private int bbcCount = 0;
  private int scsCountForBot8 = 0;

  // for suspected agent
  private int totalSessionCnt = 0;
  private int nocguidSessionCnt = 0;
  private int spsSessionCnt = 0;
  private int nouidSessionCnt = 0;
  private int directSessionCnt = 0;
  private int mktgSessionCnt = 0;
  private int ipCountForSuspect = 0;

  // for suspected IP
  private int totalCnt = 0;
  private int validPageCnt = -1;
  private int maxValidPageCnt = -1;
  private boolean consistent = true;
  private int homePageCnt = 0;
  private int familyViCnt = 0;
  private int signinCnt = 0;
  private int noUidCnt = 0;
  private int directCnt = 0;
  private int mktgCnt = 0;
  private int siteCnt = 0;
  private int newGuidCnt = 0;
  //    private int guidCnt = 0;
  private Set<Guid> cguidSet = new HashSet<>();
  //   private Set<Guid> guidSet = new CopyOnWriteArraySet<Guid>();
  //  private HllSketch guidSet = new HllSketch(20, TgtHllType.HLL_8);
  private byte[] hllSketch;
  private Boolean isAllAgentHoper = true;
  private int totalCntForSec1 = 0;


  //for new agent+Ip bot sync with clav_session
  //private int sessionCntForAgentIp5 = 0;
  //private byte[] guidSetForAgentIp5;
  private int isAgentIp5Bot = 0;
  private int sessionCntForAgentIp8 = 0;
  private int newGuidCntForAgentIp8 = 0;
  private int isAgentIp8Bot = 0;


  // for newbot
  private boolean hasNewBot = false;

  public AgentIpAttribute() {
  }

  @Override
  public void feed(SessionCore sessionCore, int botFlag) {
    if (this.timestamp < sessionCore.getAbsStartTimestamp()) {
      this.timestamp = sessionCore.getAbsStartTimestamp();
    }
    switch (botFlag) {

      //      case 20: {
      //        sessionCntForAgentIp5 += 1;
      //        HllSketch guidSet;
      //        if (guidSetForAgentIp5 == null) {
      //          guidSet = new HllSketch(12, TgtHllType.HLL_8);
      //        } else {
      //          guidSet = HllSketch.heapify(guidSetForAgentIp5);
      //        }
      //        long[] guidList = {sessionCore.getGuid().getGuid1(),
      //            sessionCore.getGuid().getGuid2()};
      //        guidSet.update(guidList);
      //        guidSetForAgentIp5 = guidSet.toCompactByteArray();
      //        break;
      //      }

      case 21: {
        sessionCntForAgentIp8 += 1;
        if (SessionCoreHelper.isNewGuid(sessionCore)) {
          newGuidCntForAgentIp8 += 1;
        }
        break;
      }

      case 211: {
        if (!hasNewBot && REF_211.contains(sessionCore.getBotFlag())) {
          hasNewBot = true;
        }
        break;
      }
      case 202: {
        totalSessionCnt += 1;
        if (sessionCore.getCguid() == null || (sessionCore.getGuid().getGuid1() == 0L
            && sessionCore.getGuid().getGuid2() == 0L)) {
          nocguidSessionCnt += 1;
        }

        if (SessionCoreHelper.isSps(sessionCore)) {
          spsSessionCnt += 1;
        }

        if (SessionCoreHelper.isNoUid(sessionCore)) {
          nouidSessionCnt += 1;
        }

        if (SessionCoreHelper.isDirect(sessionCore)) {
          directSessionCnt += 1;
        }
        if (SessionCoreHelper.isMktg(sessionCore)) {
          mktgSessionCnt += 1;
        }

        if (SessionCoreHelper.getExInternalIp(sessionCore) != null) {
          ipCountForSuspect = 1;
        }
        break;
      }

      case 210: {
        totalCnt += 1;
        if (validPageCnt >= 0) {
          consistent =
              consistent
                  && (validPageCnt == sessionCore.getValidPageCnt()
                  || validPageCnt < 0
                  || sessionCore.getValidPageCnt() < 0);
        } else {
          consistent = true;
        }

        if (sessionCore.getValidPageCnt() >= 0) {
          validPageCnt = sessionCore.getValidPageCnt();
        }
        maxValidPageCnt = Math.max(maxValidPageCnt, sessionCore.getValidPageCnt());

        if (SessionCoreHelper.isHomePage(sessionCore)) {
          homePageCnt += 1;
        }
        if (SessionCoreHelper.isFamilyVi(sessionCore)) {
          familyViCnt += 1;
        }
        if (SessionCoreHelper.isSignIn(sessionCore)) {
          signinCnt += 1;
        }
        if (SessionCoreHelper.isNoUid(sessionCore)) {
          noUidCnt += 1;
        }
        if (SessionCoreHelper.isDirect(sessionCore)) {
          directCnt += 1;
        }
        if (SessionCoreHelper.isMktg(sessionCore)) {
          mktgCnt += 1;
        }
        if (SessionCoreHelper.isSite(sessionCore)) {
          siteCnt += 1;
        }
        if (SessionCoreHelper.isNewGuid(sessionCore)) {
          newGuidCnt += 1;
        }
        if (sessionCore.getGuid() != null) {
          //          Guid guid = new Guid();
          //          guid.setGuid1(sessionCore.getGuid().getGuid1());
          //          guid.setGuid2(sessionCore.getGuid().getGuid2());
          //          guidSet.add(guid);
          HllSketch guidSet;
          if (hllSketch == null) {
            guidSet = new HllSketch(12, TgtHllType.HLL_4);
          } else {
            guidSet = HllSketch.heapify(hllSketch);
          }
          long[] guidList = {sessionCore.getGuid().getGuid1(),
              sessionCore.getGuid().getGuid2()};
          guidSet.update(guidList);
          hllSketch = guidSet.toCompactByteArray();
        }
        if (sessionCore.getCguid() != null) {
          if (cguidSet.size() <= 5) {
            Guid guid = new Guid();
            guid.setGuid1(sessionCore.getCguid().getGuid1());
            guid.setGuid2(sessionCore.getCguid().getGuid2());
            cguidSet.add(guid);
          }
        }
        isAllAgentHoper = isAllAgentHoper && SessionCoreHelper.isAgentHoper(sessionCore);
        break;
      }
      case 5:
        scsCountForBot5 += 1;
        break;
      case 6: {
        scsCountForBot6 += 1;
        //        if (SessionCoreHelper.getExInternalIp(sessionCore) != null) {
        //          ipCount = 1;
        //        }
        break;
      }
      case 7:
        scsCountForBot7 += 1;
        break;
      case 8: {
        scsCountForBot8 += 1;
        break;
      }
      default:
        break;
    }
  }


  public void merge(AgentIpAttribute agentIpAttribute, int botFlag) {
    if (this.timestamp < agentIpAttribute.getTimestamp()) {
      this.timestamp = agentIpAttribute.getTimestamp();
    }
    switch (botFlag) {
      //      case 20: {
      //        sessionCntForAgentIp5 += agentIpAttribute.getSessionCntForAgentIp5();
      //        Union guidSet;
      //        if (guidSetForAgentIp5 == null) {
      //          guidSet = new Union();
      //        } else {
      //          guidSet = Union.heapify(guidSetForAgentIp5);
      //        }
      //
      //        if (agentIpAttribute.getGuidSetForAgentIp5() != null) {
      //          HllSketch guidSet2 = HllSketch.heapify(agentIpAttribute.getGuidSetForAgentIp5());
      //          guidSet.update(guidSet2);
      //        }
      //        guidSetForAgentIp5 = guidSet.toCompactByteArray();
      //
      //        if (sessionCntForAgentIp5 > 100
      //            && guidSet.getEstimate() >= 0.98 * sessionCntForAgentIp5) {
      //          isAgentIp5Bot = 1;
      //        } else {
      //          isAgentIp5Bot = 0;
      //        }
      //        break;
      //      }

      case 21: {
        sessionCntForAgentIp8 += agentIpAttribute.getSessionCntForAgentIp8();
        newGuidCntForAgentIp8 += agentIpAttribute.getNewGuidCntForAgentIp8();
        if (sessionCntForAgentIp8 > 200
            && newGuidCntForAgentIp8 >= 0.97 * sessionCntForAgentIp8) {
          isAgentIp8Bot = 1;
        } else {
          isAgentIp8Bot = 0;
        }
        break;
      }

      case 5: {
        if (scsCountForBot5 < 0) {
          break;
        } else if (agentIpAttribute.getScsCountForBot5() < 0) {
          scsCountForBot5 = -1;
        } else {
          scsCountForBot5 += agentIpAttribute.getScsCountForBot5();
        }
        break;
      }
      case 6: {
        //         if (ipCount > 0 || (agentIpAttribute.getIpCount() > 0)) {
        //          ipCount = 1;
        //        } else {
        //          ipCount = 0;
        //        }
        ipSet.addAll(agentIpAttribute.getIpSet());
        if (scsCountForBot6 < 0) {
          break;
        } else if (agentIpAttribute.getScsCountForBot6() < 0) {
          scsCountForBot6 = -1;
        } else {
          scsCountForBot6 += agentIpAttribute.getScsCountForBot6();
        }
        break;
      }
      case 7: {
        if (scsCountForBot7 < 0) {
          break;
        } else if (agentIpAttribute.getScsCountForBot7() < 0) {
          scsCountForBot7 = -1;
        } else {
          scsCountForBot7 += agentIpAttribute.getScsCountForBot7();
        }
        break;
      }
      case 8: {
        bbcCount += agentIpAttribute.getBbcCount();
        if (scsCountForBot8 < 0) {
          break;
        } else if (agentIpAttribute.getScsCountForBot8() < 0) {
          scsCountForBot8 = -1;
        } else {
          scsCountForBot8 += agentIpAttribute.getScsCountForBot8();
        }

        break;
      }
      case 202: {
        totalSessionCnt += agentIpAttribute.getTotalSessionCnt();
        nocguidSessionCnt += agentIpAttribute.getNocguidSessionCnt();
        spsSessionCnt += agentIpAttribute.getSpsSessionCnt();
        nouidSessionCnt += agentIpAttribute.getNouidSessionCnt();
        directSessionCnt += agentIpAttribute.getDirectSessionCnt();
        mktgSessionCnt += agentIpAttribute.getMktgSessionCnt();
        if (ipCountForSuspect > 0 || (agentIpAttribute.getIpCountForSuspect() > 0)) {
          ipCountForSuspect = 1;
        } else {
          ipCountForSuspect = 0;
        }
        break;
      }
      case 210: {
        isAllAgentHoper = isAllAgentHoper && agentIpAttribute.getIsAllAgentHoper();
        totalCnt += agentIpAttribute.totalCnt;
        consistent = consistent
            && agentIpAttribute.consistent
            && (validPageCnt == agentIpAttribute.getValidPageCnt()
            || validPageCnt < 0 || agentIpAttribute.getValidPageCnt() < 0);

        if (agentIpAttribute.getValidPageCnt() >= 0) {
          validPageCnt = agentIpAttribute.getValidPageCnt();
        }
        maxValidPageCnt = Math.max(maxValidPageCnt,
            agentIpAttribute.getMaxValidPageCnt());

        homePageCnt += agentIpAttribute.getHomePageCnt();
        familyViCnt += agentIpAttribute.getFamilyViCnt();
        signinCnt += agentIpAttribute.getSigninCnt();
        noUidCnt += agentIpAttribute.getNoUidCnt();
        directCnt += agentIpAttribute.getDirectCnt();
        mktgCnt += agentIpAttribute.getMktgCnt();
        siteCnt += agentIpAttribute.getSiteCnt();
        newGuidCnt += agentIpAttribute.getNewGuidCnt();

        //        HllSketch guidSet;
        //        if (hllSketch == null && agentIpAttribute.getHllSketch() == null) {
        //          guidSet = new HllSketch(12, TgtHllType.HLL_4);
        //          for (Guid guid : this.getGuidSet()) {
        //            long[] guidList = {guid.getGuid1(),
        //                guid.getGuid2()};
        //            guidSet.update(guidList);
        //          }
        //          getGuidSet().clear();
        //          for (Guid guid : agentIpAttribute.getGuidSet()) {
        //            long[] guidList = {guid.getGuid1(),
        //                guid.getGuid2()};
        //            guidSet.update(guidList);
        //          }
        //          agentIpAttribute.getGuidSet().clear();
        //        } else if (hllSketch == null && agentIpAttribute.getHllSketch() != null) {
        //
        //          guidSet = HllSketch.heapify(agentIpAttribute.getHllSketch());
        //          for (Guid guid : this.getGuidSet()) {
        //            long[] guidList = {guid.getGuid1(),
        //                guid.getGuid2()};
        //            guidSet.update(guidList);
        //          }
        //        } else if (hllSketch != null && agentIpAttribute.getHllSketch() == null) {
        //          guidSet = HllSketch.heapify(hllSketch);
        //          for (Guid guid : agentIpAttribute.getGuidSet()) {
        //            long[] guidList = {guid.getGuid1(),
        //                guid.getGuid2()};
        //            guidSet.update(guidList);
        //          }
        //        } else {
        //          guidSet = HllSketch.heapify(hllSketch);
        //        }
        //        hllSketch = guidSet.toCompactByteArray();
        if (cguidSet.size() < 5) {
          cguidSet.addAll(agentIpAttribute.getCguidSet());
        }
        break;
      }
      case 211: {
        hasNewBot = hasNewBot || agentIpAttribute.isHasNewBot();
        break;
      }
      default:
        break;
    }
  }

  @Override
  public void revert(SessionCore sessionCore, int botFlag) {
    switch (botFlag) {
      case 5:
        scsCountForBot5 = -1;
        break;
      case 6:
        scsCountForBot6 = -1;
        break;
      case 7:
        scsCountForBot7 = -1;
        break;
      case 8:
        scsCountForBot8 = -1;
        break;
      default:
        break;
    }
  }

  @Override
  public void clear() {
    scsCountForBot5 = 0;
    scsCountForBot6 = 0;
    scsCountForBot7 = 0;
    scsCountForBot8 = 0;
    ipSet.clear();
    bbcCount = 0;
    totalSessionCnt = 0;
    nocguidSessionCnt = 0;
    spsSessionCnt = 0;
    nouidSessionCnt = 0;
    directSessionCnt = 0;
    mktgSessionCnt = 0;
    ipCountForSuspect = 0;
    totalCnt = 0;
    validPageCnt = -1;
    maxValidPageCnt = -1;
    consistent = true;
    homePageCnt = 0;
    familyViCnt = 0;
    signinCnt = 0;
    noUidCnt = 0;
    directCnt = 0;
    mktgCnt = 0;
    siteCnt = 0;
    newGuidCnt = 0;
    //        guidCnt = 0;
    cguidSet.clear();
    hllSketch = null;
    isAllAgentHoper = true;
    totalCntForSec1 = 0;
  }

  @Override
  public void clear(int botFlag) {
    switch (botFlag) {
      case 5:
        scsCountForBot5 = 0;
        break;
      case 6:
        scsCountForBot6 = 0;
        ipSet.clear();
        break;
      case 7:
        scsCountForBot7 = 0;
        break;
      case 8:
        scsCountForBot8 = 0;
        bbcCount = 0;
        break;
      case 202:
        totalSessionCnt = 0;
        nocguidSessionCnt = 0;
        spsSessionCnt = 0;
        nouidSessionCnt = 0;
        directSessionCnt = 0;
        mktgSessionCnt = 0;
        ipCountForSuspect = 0;
        break;
      case 210:
        totalCnt = 0;
        validPageCnt = -1;
        maxValidPageCnt = -1;
        consistent = true;
        homePageCnt = 0;
        familyViCnt = 0;
        signinCnt = 0;
        noUidCnt = 0;
        directCnt = 0;
        mktgCnt = 0;
        siteCnt = 0;
        newGuidCnt = 0;
        cguidSet.clear();
        /*guidSet.clear();*/
        hllSketch = null;
        isAllAgentHoper = true;
        totalCntForSec1 = 0;
        break;
      default:
        break;
    }
  }
}

