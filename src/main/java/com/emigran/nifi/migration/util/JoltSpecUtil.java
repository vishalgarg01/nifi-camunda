package com.emigran.nifi.migration.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Analyzes a JOLT transformation spec to determine whether it expects
 * the input to be a single JSON object {@code {}} or an array of objects {@code [{}]}.
 * <p>
 * Heuristic: In JOLT "shift", the root level of the spec describes the expected
 * input root. If the spec has a root-level key {@code "*"}, the transform iterates
 * over array elements, so the input is expected to be an array. Otherwise the
 * input is expected to be an object.
 */
public final class JoltSpecUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public enum JoltInputShape {
        /** JOLT expects a single JSON object at root. */
        OBJECT,
        /** JOLT expects an array of JSON objects at root. */
        ARRAY,
        /** Could not determine (invalid or empty spec). */
        UNKNOWN
    }

    private JoltSpecUtil() {
    }

    /**
     * Determines whether the given JOLT spec string expects root input to be an object or an array.
     *
     * @param joltSpecJson JOLT specification as JSON string (array of operations or single operation object)
     * @return OBJECT if spec expects {@code {}}, ARRAY if it expects {@code [{}]}, UNKNOWN otherwise
     */
    public static JoltInputShape getExpectedInputShape(String joltSpecJson) {
        if (joltSpecJson == null || joltSpecJson.trim().isEmpty()) {
            return JoltInputShape.UNKNOWN;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(joltSpecJson.trim());
            JsonNode shiftSpec = findFirstShiftSpec(root);
            if (shiftSpec == null || !shiftSpec.isObject()) {
                return JoltInputShape.UNKNOWN;
            }
            return rootSpecExpectsArray(shiftSpec) ? JoltInputShape.ARRAY : JoltInputShape.OBJECT;
        } catch (IOException e) {
            return JoltInputShape.UNKNOWN;
        }
    }

    /**
     * Returns the first "shift" operation's "spec" node.
     * Accepts either a JSON array of operations or a single operation object.
     */
    private static JsonNode findFirstShiftSpec(JsonNode root) {
        if (root.isArray()) {
            for (JsonNode op : root) {
                JsonNode spec = getShiftSpecFromOperation(op);
                if (spec != null) return spec;
            }
            return null;
        }
        if (root.isObject()) {
            return getShiftSpecFromOperation(root);
        }
        return null;
    }

    private static JsonNode getShiftSpecFromOperation(JsonNode op) {
        if (op == null || !op.isObject()) return null;
        JsonNode operation = op.get("operation");
        if (operation == null || !operation.isTextual() || !"shift".equals(operation.asText())) {
            return null;
        }
        JsonNode spec = op.get("spec");
        return (spec != null && spec.isObject()) ? spec : null;
    }

    /**
     * Returns true if the root level of the shift spec has "*" (wildcard),
     * meaning the transform expects the input root to be an array.
     */
    private static boolean rootSpecExpectsArray(JsonNode specRoot) {
        Iterator<String> keys = specRoot.fieldNames();
        while (keys.hasNext()) {
            String key = keys.next();
            if ("*".equals(key)) {
                return true;
            }
            // Optional: treat numeric root keys as array (e.g. "0", "1" for array indices)
            if (isNumeric(key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNumeric(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (i == 0 && s.charAt(0) == '-') continue;
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return true;
    }

    /**
     * CLI: reads JOLT spec from a file or stdin and prints OBJECT, ARRAY, or UNKNOWN.
     * Usage: java JoltSpecUtil [path-to-spec.json]
     *        echo '[{"operation":"shift","spec":{"*":{...}}}]' | java JoltSpecUtil
     */
    public static void main(String[] args) {
        String json;
        try {
            if (args.length >= 1) {
                Path path = Paths.get(args[0]);
                json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            } else {
                json = readFully(System.in);
            }
        } catch (IOException e) {
            System.err.println("Error reading input: " + e.getMessage());
            System.exit(1);
            return;
        }
        JoltInputShape shape = getExpectedInputShape(json);
        System.out.println(shape.name());
    }

    private static String readFully(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
