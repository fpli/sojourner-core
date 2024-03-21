package com.ebay.sojourner.cjs.service;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CjsMetadataManagerTest {

    @Test
    void testNewInstanceNotNull() {
        val signalBetaDefinitions = CjsMetadataManager.getCjsBetaMetadataProvider().get();
        assertNotNull(signalBetaDefinitions);

        val signalDefinitions = CjsMetadataManager.getCjsMetadataProvider().get();
        assertNotNull(signalDefinitions);
    }

    @Test
    void testCloseInstance() {
        CjsMetadataManager.getInstance().close();
    }
}