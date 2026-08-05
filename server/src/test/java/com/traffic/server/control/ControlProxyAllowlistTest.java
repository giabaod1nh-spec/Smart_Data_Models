package com.traffic.server.control;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlProxyAllowlistTest {

    private final ControlProxyAllowlist allowlist = new ControlProxyAllowlist(defaultProps(false, false));

    private static com.traffic.server.config.AppProperties defaultProps(boolean domain, boolean adapters) {
        return new com.traffic.server.config.AppProperties(
                new com.traffic.server.config.AppProperties.ControlApi("http://x", 1000, 65536, ""),
                new com.traffic.server.config.AppProperties.Control(domain, false, adapters),
                new com.traffic.server.config.AppProperties.ContextProvider("http://x"),
                new com.traffic.server.config.AppProperties.Realtime(150, 0.0001, 2.0),
                new com.traffic.server.config.AppProperties.Security(
                        new com.traffic.server.config.AppProperties.Security.Admin("a", "b")));
    }

    @Test
    void blocksMutationRoutesWhenAdaptersEnabled() {
        ControlProxyAllowlist adapted = new ControlProxyAllowlist(defaultProps(true, true));
        assertFalse(adapted.isAllowed("/phase"));
        assertFalse(adapted.isAllowed("/overlays/x"));
        assertTrue(adapted.isAllowed("/health"));
        assertTrue(adapted.isAllowed("/overlays"));
    }

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
