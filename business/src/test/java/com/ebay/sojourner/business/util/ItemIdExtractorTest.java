package com.ebay.sojourner.business.util;

import com.ebay.sojourner.common.model.UbiEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ItemIdExtractorTest {

    @Test
    void test_extracItemId_Positive() {
        assertEquals("1234567890", ItemIdExtractor.extractItemId("https://www.ebay.com/itm/1234567890"));
        assertEquals("256190301478",
                ItemIdExtractor
                        .extractItemId(
                                "https://www.ebay.com/itm/256190301478?" +
                                        "itmmeta=01HQVFNE244ZMVE4KAXT3ZGXJ4&hash=item3ba621c526:" +
                                        "g:DIkAAOSwZehkAtQf&itmprp=enc%3AAQAIAAAA8L361VKYaBa%2FMNabeUm%2" +
                                        "Fcqg5n1GzlMj5GXPwNwxFUASbAaBE4ptj034OEW5zZy2slZktnBtvbEH6mUDrdzCUH" +
                                        "YuO2LdxX37qZFvfYFX6ZR53aQkW%2B9B%2B6JUPlQHbkrYCF0ztitPrnYZJzKbXaxAwE" +
                                        "GCVG4gy57FOlXeUjVStElEckyjyyw1OA6nvuIS37nr%2Bu75OpQbnL988oUZEc9VD7%2" +
                                        "BLGvGcZ6fUKXsO%2BgvdM8fbttam%2FxjUVkTWgKxVdNH95KuIERCDDmo9furHjurwy" +
                                        "WjMNeaAU6Ne6AEACgEOje29CX6BHx2auLlt9RuUTdiCHBQ%3D%3D%7Ctkp%3ABk9SR5Th1e--Yw"));
        assertEquals("174507208109", ItemIdExtractor
                .extractItemId("https://www.ebay.ca/itm/Harry-Potter-A-Pop-Up-Guide-to-Hogwarts-/174507208109"));

    }

    @Test
    void test_extracItemId_negative() {
        assertNull(ItemIdExtractor.extractItemId("https://www.ebay.com/itm/"));
        assertNull(ItemIdExtractor.extractItemId(null));
    }

    @Test
    void test_accept_positive() {
        UbiEvent ubiEvent = new UbiEvent();
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1=200");
        assertTrue(ItemIdExtractor.accept(ubiEvent));
    }

    @Test
    void test_accept_negative() {
        UbiEvent ubiEvent = new UbiEvent();
        ubiEvent.setRdt(true);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1=200");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208338);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1=200");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48377&ex1=200");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1=0");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("&ex1=0");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&&ex1=-10");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
        ubiEvent.setItemId(1234567890L);
        ubiEvent.setRdt(false);
        ubiEvent.setPageId(2208336);
        ubiEvent.setApplicationPayload("moduledtl=mi%3A48379&ex1=200");
        assertFalse(ItemIdExtractor.accept(ubiEvent));
    }

}
