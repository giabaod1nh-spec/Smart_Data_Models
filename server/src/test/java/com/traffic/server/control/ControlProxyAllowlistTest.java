package com.traffic.server.control;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlProxyAllowlistTest {

    private final ControlProxyAllowlist allowlist = new ControlProxyAllowlist();

    @Test
    void allowsKnownRoutes() {
        assertTrue(allowlist.isAllowed("/health"));
        assertTrue(allowlist.isAllowed("/scenario"));
        assertTrue(allowlist.isAllowed("/overlays/abc"));
        assertTrue(allowlist.isAllowed("/intersections/A/state"));
        assertTrue(allowlist.isAllowed("/links/W1J1/state"));
        assertTrue(allowlist.isAllowed("/snapshot/A"));
        assertTrue(allowlist.isAllowed("/trip-records"));
        assertTrue(allowlist.isAllowed("/stats"));
    }

    @Test
    void rejectsUnknownOrTraversalPaths() {
        assertFalse(allowlist.isAllowed("/admin"));
        assertFalse(allowlist.isAllowed("/../health"));
        assertFalse(allowlist.isAllowed("/internal/debug"));
    }

    @Test
    void extractsUpstreamPath() {
        assertTrue("/scenario".equals(ControlProxyAllowlist.extractUpstreamPath("/api/control/scenario")));
        assertTrue("/overlays/x".equals(ControlProxyAllowlist.extractUpstreamPath("/api/control/overlays/x")));
    }
}
