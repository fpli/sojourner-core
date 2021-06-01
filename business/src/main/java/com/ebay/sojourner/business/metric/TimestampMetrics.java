package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.business.parser.PageIndicator;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.IntermediateLkp;
import com.ebay.sojourner.common.util.IsValidIPv4;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.ebay.sojourner.common.util.SojTimestamp;
import com.ebay.sojourner.common.util.SojUtils;
import com.ebay.sojourner.common.util.UBIConfig;
import java.util.Set;

public class TimestampMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  public static final String SHOCKWAVE_FLASH_AGENT = "Shockwave Flash";
  private PageIndicator indicator;
  private Set<Integer> agentExcludeSet;

  @Override
  public void start(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession().setAbsStartTimestamp(null);
    sessionAccumulator.getUbiSession().setAbsEndTimestamp(null);
    sessionAccumulator.getUbiSession().setStartTimestamp(null);
    sessionAccumulator.getUbiSession().setEndTimestamp(null);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    UbiSession ubiSession = sessionAccumulator.getUbiSession();
    if (!event.isIframe()) {
      if (!event.isRdt() || indicator.isCorrespondingPageEvent(event)) {
        if (ubiSession.getStartTimestamp() == null) {
          ubiSession.setStartTimestamp(event.getEventTimestamp());
        } else if (event.getEventTimestamp() != null
                  && ubiSession.getStartTimestamp() > event.getEventTimestamp()) {
          ubiSession.setStartTimestamp(event.getEventTimestamp());
        }
        if (ubiSession.getEndTimestamp() == null) {
          ubiSession.setEndTimestamp(event.getEventTimestamp());
        } else if (event.getEventTimestamp() != null
                  && ubiSession.getEndTimestamp() < event.getEventTimestamp()) {
          ubiSession.setEndTimestamp(event.getEventTimestamp());
        }
      }
      if (!event.isRdt()) {
        if (ubiSession.getStartTimestampNOIFRAMERDT() == null) {
          ubiSession.setStartTimestampNOIFRAMERDT(event.getEventTimestamp());
          event.setEntryPage(true);
        } else if (event.getEventTimestamp() != null
                  && ubiSession.getStartTimestampNOIFRAMERDT() > event.getEventTimestamp()) {
          ubiSession.setStartTimestampNOIFRAMERDT(event.getEventTimestamp());
        }

        if (ubiSession.getEndTimestampNOIFRAMERDT() == null) {
          ubiSession.setEndTimestampNOIFRAMERDT(event.getEventTimestamp());
        } else if (event.getEventTimestamp() != null
                  && ubiSession.getEndTimestampNOIFRAMERDT() < event.getEventTimestamp()) {
          ubiSession.setEndTimestampNOIFRAMERDT(event.getEventTimestamp());
        }

        String agentInfo = event.getAgentInfo();
        if (!agentExcludeSet.contains(event.getPageId())
            && agentInfo != null
            && !agentInfo.equals(SHOCKWAVE_FLASH_AGENT)
            && !IsValidIPv4.isValidIP(agentInfo)) {
          if (ubiSession.getStartTimestampForAgentString() == null) {
            ubiSession.setStartTimestampForAgentString(event.getEventTimestamp());
          } else if (event.getEventTimestamp() != null
                    && ubiSession.getStartTimestampForAgentString() > event.getEventTimestamp()) {
            ubiSession.setStartTimestampForAgentString(event.getEventTimestamp());
          }
        }

        // FIXME(Jason): IntermediateLkp should be replaced with a consolidated class,
        //               too many "lkp" classes in code now
        // for ScEvent
        Integer pageId = event.getPageId() == -1 ? -99 : event.getPageId();
        if (!IntermediateLkp.getInstance().getScPageSet1().contains(pageId)
            && !IntermediateLkp.getInstance().getScPageSet2().contains(pageId)) {
          if (ubiSession.getStartTimestampForScEvent() == null) {
            ubiSession.setStartTimestampForScEvent(event.getEventTimestamp());
          } else if (event.getEventTimestamp() != null
                    && ubiSession.getStartTimestampForScEvent() > event.getEventTimestamp()) {
            ubiSession.setStartTimestampForScEvent(event.getEventTimestamp());
          }
        }
      }

      if (ubiSession.getStartTimestampNOIFRAME() == null) {
        ubiSession.setStartTimestampNOIFRAME(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getStartTimestampNOIFRAME() > event.getEventTimestamp()) {
        ubiSession.setStartTimestampNOIFRAME(event.getEventTimestamp());
      }
    }

    if (event.getReferrer() != null) {
      if (ubiSession.getStartTimestampForReferrer() == null) {
        ubiSession.setStartTimestampForReferrer(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getStartTimestampForReferrer() > event.getEventTimestamp()) {
        ubiSession.setStartTimestampForReferrer(event.getEventTimestamp());
      }
    }

    if (ubiSession.getAbsStartTimestamp() == null) {
      ubiSession.setAbsStartTimestamp(event.getEventTimestamp());
    } else if (event.getEventTimestamp() != null
              && ubiSession.getAbsStartTimestamp() > event.getEventTimestamp()) {
      ubiSession.setAbsStartTimestamp(event.getEventTimestamp());
    }

    if (ubiSession.getAbsEndTimestamp() == null) {
      ubiSession.setAbsEndTimestamp(event.getEventTimestamp());
    } else if (event.getEventTimestamp() != null
              && ubiSession.getAbsEndTimestamp() < event.getEventTimestamp()) {
      ubiSession.setAbsEndTimestamp(event.getEventTimestamp());
    }

    // for roverclick
    if (SojUtils.isRoverClick(event)) {
      if (ubiSession.getAbsStartTimestampForRoverClick() == null) {
        ubiSession.setAbsStartTimestampForRoverClick(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForRoverClick() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForRoverClick(event.getEventTimestamp());
      }
    }

    // for rover3084
    if (SojUtils.isRover3084Click(event)) {
      if (ubiSession.getAbsStartTimestampForRover3084() == null) {
        ubiSession.setAbsStartTimestampForRover3084(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForRover3084() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForRover3084(event.getEventTimestamp());
      }
    }

    // for rover3085
    if (SojUtils.isRover3085Click(event)) {
      if (ubiSession.getAbsStartTimestampForRover3085() == null) {
        ubiSession.setAbsStartTimestampForRover3085(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForRover3085() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForRover3085(event.getEventTimestamp());
      }
    }

    // for rover3962
    if (SojUtils.isRover3962Click(event)) {
      if (ubiSession.getAbsStartTimestampForRover3962() == null) {
        ubiSession.setAbsStartTimestampForRover3962(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForRover3962() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForRover3962(event.getEventTimestamp());
      }
    }

    // for NotifyClick
    if (IntermediateLkp.getInstance()
                       .getNotifyCLickPageSet()
                       .contains(event.getPageId())) {
      if (ubiSession.getAbsStartTimestampForNotifyClick() == null) {
        ubiSession.setAbsStartTimestampForNotifyClick(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForNotifyClick() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForNotifyClick(event.getEventTimestamp());
      }
    }

    // for NotifyView
    if (IntermediateLkp.getInstance()
                       .getNotifyViewPageSet()
                       .contains(event.getPageId())) {
      if (ubiSession.getAbsStartTimestampForNotifyView() == null) {
        ubiSession.setAbsStartTimestampForNotifyView(event.getEventTimestamp());
      } else if (event.getEventTimestamp() != null
                && ubiSession.getAbsStartTimestampForNotifyView() > event.getEventTimestamp()) {
        ubiSession.setAbsStartTimestampForNotifyView(event.getEventTimestamp());
      }
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession()
                      .setSojDataDt(sessionAccumulator.getUbiSession()
                                                      .getAbsEndTimestamp());
    // add logic to feed sessionenddt
    sessionAccumulator.getUbiSession()
        .setSessionEndDt(SojTimestamp.getUnixTimestamp(sessionAccumulator.getUbiSession()
            .getAbsEndTimestamp()));
    // Fix bug HDMIT-3732 to avoid integer result overflow
    int durationSec =
        (sessionAccumulator.getUbiSession().getStartTimestamp() == null
            || sessionAccumulator.getUbiSession().getEndTimestamp() == null)
            ? 0
            : (int)
            ((sessionAccumulator.getUbiSession().getEndTimestamp()
                - sessionAccumulator.getUbiSession().getStartTimestamp())
                / 1000000);

    long absDuration =
        (sessionAccumulator.getUbiSession().getAbsEndTimestamp() == null
            || sessionAccumulator.getUbiSession().getAbsStartTimestamp() == null
            ? 0 : sessionAccumulator.getUbiSession().getAbsEndTimestamp()
            - sessionAccumulator.getUbiSession().getAbsStartTimestamp());
    sessionAccumulator.getUbiSession().setDurationSec(durationSec);
    sessionAccumulator.getUbiSession().setAbsDuration(absDuration);
  }

  @Override
  public void init() throws Exception {
    indicator = new PageIndicator(UBIConfig.getString(Property.SEARCH_VIEW_PAGES));
    agentExcludeSet = PropertyUtils.getIntegerSet(
        UBIConfig.getString(Property.AGENT_EXCLUDE_PAGES), Property.PROPERTY_DELIMITER);
  }
}
