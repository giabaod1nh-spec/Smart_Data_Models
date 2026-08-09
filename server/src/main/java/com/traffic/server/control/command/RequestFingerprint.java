package com.traffic.server.control.command;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/** SHA-256 fingerprint of canonical command request material (RC1-T3). */
public final class RequestFingerprint {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RequestFingerprint() {}

    public static String compute(
            String contractVersion,
            String commandType,
            JsonNode target,
            JsonNode payload,
            String expectedRunId,
            String source) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("contractVersion", contractVersion);
        root.put("commandType", commandType);
        root.set("target", target == null ? MAPPER.createObjectNode() : target);
        root.set("payload", payload == null ? MAPPER.createObjectNode() : payload);
        root.put("expectedRunId", expectedRunId);
        root.put("source", source);
        String canonical = canonicalJson(root);
        return sha256Hex(canonical);
    }

    static String canonicalJson(JsonNode node) {
        try {
            if (node.isObject()) {
                TreeMap<String, JsonNode> sorted = new TreeMap<>();
                node.properties().forEach(e -> sorted.put(e.getKey(), e.getValue()));
                ObjectNode out = MAPPER.createObjectNode();
                for (Map.Entry<String, JsonNode> e : sorted.entrySet()) {
                    out.set(e.getKey(), MAPPER.readTree(canonicalJson(e.getValue())));
                }
                return MAPPER.writeValueAsString(out);
            }
            if (node.isArray()) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < node.size(); i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(canonicalJson(node.get(i)));
                }
                sb.append(']');
                return sb.toString();
            }
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalStateException("canonical json failed", e);
        }
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("sha256 failed", e);
        }
    }
}
