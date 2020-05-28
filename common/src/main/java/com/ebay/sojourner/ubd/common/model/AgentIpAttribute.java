package com.ebay.sojourner.ubd.common.model;

import com.ebay.sojourner.ubd.common.util.SessionCoreHelper;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Data;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

@Data
public class AgentIpAttribute implements Attribute<SessionCore>, Serializable {

  private Integer clientIp;
  private AgentHash agent;
  private Set<Integer> botFlagList = new LinkedHashSet<>();
  private int scsCountForBot5 = 0;
  private int scsCountForBot6 = 0;
  private int scsCountForBot7 = 0;
  private int scsCountForBot8 = 0;
  private int ipCount = 0;
  private int bbcCount = 0;

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
  //  private Set<Guid> guidSet = new HashSet<Guid>();
  private HllSketch guidSet = new HllSketch(20, TgtHllType.HLL_8);
  private byte[] hllSketch;
  private Boolean isAllAgentHoper = true;
  private int totalCntForSec1 = 0;

  public AgentIpAttribute() {
  }

  @Override
  public void feed(SessionCore intermediateSession, int botFlag, boolean isNeeded) {
    if (isNeeded) {
      totalSessionCnt += 1;
      if (intermediateSession.getCguid() == null) {
        nocguidSessionCnt += 1;
      }

      if (SessionCoreHelper.isSps(intermediateSession)) {
        spsSessionCnt += 1;
      }

      if (SessionCoreHelper.isNoUid(intermediateSession)) {
        nouidSessionCnt += 1;
      }

      if (SessionCoreHelper.isDirect(intermediateSession)) {
        directSessionCnt += 1;
      }
      if (SessionCoreHelper.isMktg(intermediateSession)) {
        mktgSessionCnt += 1;
      }

      if (SessionCoreHelper.getExInternalIp(intermediateSession) != null) {
        ipCountForSuspect = 1;
      }

      if (SessionCoreHelper.getExInternalIp(intermediateSession) != null) {
        ipCount = 1;
      }
      totalCnt += 1;
      consistent =
          consistent
              && (validPageCnt == intermediateSession.getValidPageCnt()
              || validPageCnt < 0
              || intermediateSession.getValidPageCnt() < 0);

      if (intermediateSession.getValidPageCnt() >= 0) {
        validPageCnt = intermediateSession.getValidPageCnt();
      }
      maxValidPageCnt = Math.max(maxValidPageCnt, intermediateSession.getValidPageCnt());

      if (SessionCoreHelper.isHomePage(intermediateSession)) {
        homePageCnt += 1;
      }
      if (SessionCoreHelper.isFamilyVi(intermediateSession)) {
        familyViCnt += 1;
      }
      if (SessionCoreHelper.isSignIn(intermediateSession)) {
        signinCnt += 1;
      }
      if (SessionCoreHelper.isNoUid(intermediateSession)) {
        noUidCnt += 1;
      }
      if (SessionCoreHelper.isDirect(intermediateSession)) {
        directCnt += 1;
      }
      if (SessionCoreHelper.isMktg(intermediateSession)) {
        mktgCnt += 1;
      }
      if (SessionCoreHelper.isSite(intermediateSession)) {
        siteCnt += 1;
      }
      if (SessionCoreHelper.isNewGuid(intermediateSession)) {
        newGuidCnt += 1;
      }
      if (intermediateSession.getGuid() != null) {
        HllSketch guidSet;
        if (hllSketch == null) {
          guidSet = new HllSketch(12, TgtHllType.HLL_4);
        } else {
          guidSet = HllSketch.heapify(hllSketch);
        }
        long[] guidList = {intermediateSession.getGuid().getGuid1(),
            intermediateSession.getGuid().getGuid2()};
        guidSet.update(guidList);
        hllSketch = guidSet.toCompactByteArray();
      }

      if (intermediateSession.getCguid() != null) {
        if (cguidSet.size() <= 5) {
          cguidSet.add(intermediateSession.getCguid());
        }
      }
      isAllAgentHoper = isAllAgentHoper && SessionCoreHelper.isAgentHoper(intermediateSession);
    }
    switch (botFlag) {
      case 5:
        scsCountForBot5 += 1;
        break;
      case 6:
        scsCountForBot6 += 1;
        break;
      case 7:
        scsCountForBot7 += 1;
        break;
      case 8:
        scsCountForBot8 += 1;
        break;
      default:
        break;
    }
  }

  public void merge(AgentIpAttribute agentIpAttribute, int botFlag) {

    switch (botFlag) {
      case 5:
        scsCountForBot5 += agentIpAttribute.getScsCountForBot5();
        break;
      case 8:
        scsCountForBot8 += agentIpAttribute.getScsCountForBot8();
        break;
      default:
        break;
    }
  }

  @Override
  public void revert(SessionCore intermediateSession, int botFlag) {
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
    ipCount = 0;
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
        ipCount = 0;
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
      default:
        break;
    }
  }
}
