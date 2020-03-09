package com.ebay.sojourner.ubd.common.sql;

import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_1;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_10;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_11;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_12;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_13;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_2;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_3;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_4;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_5;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_56;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_6;
import static com.ebay.sojourner.ubd.common.sql.Rules.ICF_RULE_7;
import static com.ebay.sojourner.ubd.common.sql.Rules.RULE_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.ebay.sojourner.ubd.common.model.UbiEvent;
import org.junit.Test;

public class SqlEventRuleTest {

  @Test
  public void testRegex() {
    try {
      SqlEventRule rule = RULE_1;
      assertEquals(1, rule.getBotFlag(new UbiEventBuilder().agentInfo("googlebot").build()));
      assertEquals(1, rule.getBotFlag(new UbiEventBuilder().agentInfo("crawler").build()));
      assertEquals(0, rule.getBotFlag(new UbiEventBuilder().agentInfo("chrome").build()));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testUdf() {
    try {
      SqlEventRule rule = new SqlEventRule("SELECT \"square\"(2) " + "FROM \"soj\".\"ubiEvents\"");
      assertEquals(4, rule.getBotFlag(new UbiEventBuilder().build()));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testIcfAllZero() {
    try {
      long icfBinary = 0b0000000000000000;
      UbiEvent ubiEvent = new UbiEventBuilder().icfBinary(icfBinary).build();
      assertEquals(0, ICF_RULE_1.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_2.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_3.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_4.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_5.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_6.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_7.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_10.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_11.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_12.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_13.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_56.getBotFlag(ubiEvent));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testIcfAllOne() {
    try {
      long icfBinary = 0b0001111111111111 | (1 << 55);
      UbiEvent ubiEvent = new UbiEventBuilder().icfBinary(icfBinary).build();
      assertEquals(801, ICF_RULE_1.getBotFlag(ubiEvent));
      assertEquals(802, ICF_RULE_2.getBotFlag(ubiEvent));
      assertEquals(803, ICF_RULE_3.getBotFlag(ubiEvent));
      assertEquals(804, ICF_RULE_4.getBotFlag(ubiEvent));
      assertEquals(805, ICF_RULE_5.getBotFlag(ubiEvent));
      assertEquals(806, ICF_RULE_6.getBotFlag(ubiEvent));
      assertEquals(807, ICF_RULE_7.getBotFlag(ubiEvent));
      assertEquals(808, ICF_RULE_10.getBotFlag(ubiEvent));
      assertEquals(809, ICF_RULE_11.getBotFlag(ubiEvent));
      assertEquals(810, ICF_RULE_12.getBotFlag(ubiEvent));
      assertEquals(811, ICF_RULE_13.getBotFlag(ubiEvent));
      assertEquals(812, ICF_RULE_56.getBotFlag(ubiEvent));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testIcfSomeOne() {
    try {
      long icfBinary = 0b0001001000000000; // 0x1200
      UbiEvent ubiEvent = new UbiEventBuilder().icfBinary(icfBinary).build();
      assertEquals(0, ICF_RULE_1.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_2.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_3.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_4.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_5.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_6.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_7.getBotFlag(ubiEvent));
      assertEquals(808, ICF_RULE_10.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_11.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_12.getBotFlag(ubiEvent));
      assertEquals(811, ICF_RULE_13.getBotFlag(ubiEvent));
      assertEquals(0, ICF_RULE_56.getBotFlag(ubiEvent));
    } catch (Exception e) {
      fail();
    }
  }
}