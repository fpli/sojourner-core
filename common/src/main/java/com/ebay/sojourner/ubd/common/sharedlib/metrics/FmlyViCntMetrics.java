package com.ebay.sojourner.ubd.common.sharedlib.metrics;

import com.ebay.sojourner.ubd.common.model.SessionAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.sharedlib.util.SOJNVL;
import com.ebay.sojourner.ubd.common.util.LkpManager;
import com.ebay.sojourner.ubd.common.util.Property;
import com.ebay.sojourner.ubd.common.util.PropertyUtils;
import com.ebay.sojourner.ubd.common.util.UBIConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class FmlyViCntMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private List<String> viPGT;
  private LkpManager lkpFetcher;

  @Override
  public void init() throws Exception {
    this.lkpFetcher = LkpManager.getInstance();
    lkpFetcher.loadPageFmlys();
    viPGT =
        new ArrayList<>(
            PropertyUtils.parseProperty(
                UBIConfig.getString(Property.VI_EVENT_VALUES), Property.PROPERTY_DELIMITER));
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setFamilyViCnt(0);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {
    if (event.isPartialValidPage() && !event.isRdt() && !event.isIframe()) {
      Map<Integer, String[]> pageFmlyNameMap = lkpFetcher.getPageFmlyMaps();
      // im_pgt='VI': pageId=1521826 and pgt='future' or 'like'. pageId meaning?
      // or LkpPageFmlyName='VI'
      Integer pageId = event.getPageId();
      String[] pageFmly = pageFmlyNameMap.get(pageId);
      if (pageFmly != null && ("VI".equals(pageFmly[1]) || "GR/VI".equals(pageFmly[1]))) {
        sessionAccumulator
            .getUbiSession()
            .setFamilyViCnt(sessionAccumulator.getUbiSession().getFamilyViCnt() + 1);
      } else if (getImPGT(event) != null && "VI".equals(getImPGT(event))) {
        sessionAccumulator
            .getUbiSession()
            .setFamilyViCnt(sessionAccumulator.getUbiSession().getFamilyViCnt() + 1);
      }
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }

  private String getImPGT(UbiEvent event) {
    if (event.getPageId() != -1) {
      if (event.getPageId() == 1521826
          && StringUtils.isNotBlank(SOJNVL.getTagValue(event.getApplicationPayload(), "pgt"))
          && viPGT.contains(SOJNVL.getTagValue(event.getApplicationPayload(), "pgt"))) {
        return "VI";
      }
      if (event.getPageId() == 2066804
          && StringUtils.isNotBlank(event.getUrlQueryString())
          && (event.getUrlQueryString().startsWith("/itm/like")
          || event.getUrlQueryString().startsWith("/itm/future"))) {
        return "VI";
      }
      if (event.getPageId() == 1521826 || event.getPageId() == 2066804) {
        return "GR";
      }
    }
    return null;
  }
}
