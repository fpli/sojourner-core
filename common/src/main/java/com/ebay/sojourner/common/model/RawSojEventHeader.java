package com.ebay.sojourner.common.model;

import lombok.Data;

@Data
public class RawSojEventHeader {
  private boolean isValidEvent;
  private String plmt;
  private String efam;
  private String eactn;
  private boolean isEntryPage;
  private String siteId;
}
