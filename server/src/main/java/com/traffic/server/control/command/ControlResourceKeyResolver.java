package com.traffic.server.control.command;

import tools.jackson.databind.JsonNode;

import java.util.Optional;

/** Derives exclusive resource keys for database-backed locking (RC1-T4). */
public final class ControlResourceKeyResolver {

    private ControlResourceKeyResolver() {}

    public static Optional<String> resolve(ControlCommandType commandType, JsonNode target) {
        String intersectionId = textField(target, "intersectionId");
        return switch (commandType) {
            case FORCE_PHASE, SET_GREEN_DURATION -> intersectionId == null
                    ? Optional.empty()
                    : Optional.of("signal:" + intersectionId);
            case SET_SCENARIO -> Optional.of("scenario:global");
            case SET_DEMAND_PROFILE -> Optional.of("demand:global");
            case ADD_OVERLAY -> intersectionId == null
                    ? Optional.of("overlay:global")
                    : Optional.of("overlay:" + intersectionId);
            case REMOVE_OVERLAY -> {
                String overlayId = textField(target, "overlayId");
                yield overlayId == null
                        ? Optional.of("overlay:global")
                        : Optional.of("overlay:id:" + overlayId);
            }
            case SET_CONTROL_MODE, EMERGENCY_PREEMPTION -> Optional.of("control-mode:global");
        };
    }

    private static String textField(JsonNode node, String field) {
        if (node == null || node.isNull() || !node.hasNonNull(field)) {
            return null;
        }
        String value = node.get(field).asString(null);
        return value == null || value.isBlank() ? null : value;
    }
}
