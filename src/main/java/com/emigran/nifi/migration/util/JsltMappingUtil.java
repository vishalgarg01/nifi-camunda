package com.emigran.nifi.migration.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates JSLT from header mapping JSON (e.g. from HeaderProcessor "Rename Headers Mapping")
 * and resolves "Record Group By" from output names to source CSV header names for GroupByWithLimit.
 * Based on HeaderMappingToJslt / JsltMappingWithTimezone behaviour described in HEADER_TO_JSLT_MIGRATION.md.
 */
public final class JsltMappingUtil {

    private static final Pattern CONST_PATTERN = Pattern.compile("const\\{([^}]*)\\}");
    private static final Pattern HDR_IN_EXP_PATTERN = Pattern.compile("hdr\\{([^}]+)\\}");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsltMappingUtil() {
    }

    /**
     * Resolves "Record Group By" from output/mapped names to source (input) header names.
     * Use when GroupByWithLimit runs on CSV that still has original headers.
     *
     * @param recordGroupByCommaSeparated value from old pipeline (e.g. "transaction.standard.billNumber,transaction.standard.type")
     * @param headerMappingJson            same mapping as for JSLT: key = output name, value = input name
     * @return comma-separated source header names for GroupByWithLimit "Record Group By"
     */
    public static String resolveGroupByToSourceNames(String recordGroupByCommaSeparated, String headerMappingJson) throws IOException {
        if (recordGroupByCommaSeparated == null || recordGroupByCommaSeparated.trim().isEmpty()) {
            return recordGroupByCommaSeparated;
        }
        Map<String, String> mapping = OBJECT_MAPPER.readValue(
                headerMappingJson,
                new TypeReference<LinkedHashMap<String, String>>() { }
        );
        List<String> sourceNames = new ArrayList<>();
        for (String name : recordGroupByCommaSeparated.split("\\s*,\\s*")) {
            String trimmed = name.trim();
            if (trimmed.isEmpty()) continue;
            String value = mapping.get(trimmed);
            if (value == null) {
                sourceNames.add(trimmed);
                continue;
            }
            if (isSimpleFieldName(value)) {
                sourceNames.add(value);
            }
        }
        return String.join(",", sourceNames);
    }

    private static boolean isSimpleFieldName(String value) {
        if (value == null || value.isEmpty()) return false;
        String v = value.trim();
        return !v.startsWith("exp{") && !v.startsWith("const{") && !v.startsWith("hdr{")
                && !v.startsWith("sum{") && !v.startsWith("avg{");
    }

    /**
     * Generates JSLT from header mapping JSON with optional date formatting.
     *
     * @param headerMappingJson   JSON object: key = output key, value = input key or exp{}/const{}
     * @param dateColumnOutputKey output key for date column, or null to skip date formatting
     * @param existingDateFormat  e.g. "yyyy-MM-dd HH:mm:ss"
     * @param newDateFormat       e.g. "yyyy-MM-dd'T'HH:mm:ssXXX"
     * @return JSLT transform string
     */
    public static String fromHeaderMappingWithExpressions(
            String headerMappingJson,
            String dateColumnOutputKey,
            String existingDateFormat,
            String newDateFormat) throws IOException {
        return fromHeaderMappingWithExpressions(headerMappingJson, dateColumnOutputKey, existingDateFormat, newDateFormat, null);
    }

    /**
     * Same with optional timezone for the date column (e.g. "America/Chicago").
     */
    public static String fromHeaderMappingWithExpressions(
            String headerMappingJson,
            String dateColumnOutputKey,
            String existingDateFormat,
            String newDateFormat,
            String timezoneId) throws IOException {
        Map<String, String> mapping = OBJECT_MAPPER.readValue(
                headerMappingJson,
                new TypeReference<LinkedHashMap<String, String>>() { }
        );
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        int i = 0;
        int size = mapping.size();
        for (Map.Entry<String, String> e : mapping.entrySet()) {
            String outputKey = e.getKey();
            String value = e.getValue();
            String quotedKey = quoteKey(outputKey);
            String jsltExpr = parseValueToJslt(value, outputKey, dateColumnOutputKey, existingDateFormat, newDateFormat, timezoneId);
            if (jsltExpr == null) {
                jsltExpr = "null";
            }
            sb.append("  ").append(quotedKey).append(": ").append(jsltExpr);
            if (i < size - 1) {
                sb.append(",");
            }
            sb.append("\n");
            i++;
        }
        sb.append("}");
        return sb.toString();
    }

    static String parseValueToJslt(String value, String outputKey,
                                   String dateColumnOutputKey, String existingDateFormat, String newDateFormat,
                                   String timezoneId) {
        if (value == null || value.trim().isEmpty()) {
            return "null";
        }
        String v = value.trim();

        Matcher constMatcher = CONST_PATTERN.matcher(v);
        if (constMatcher.matches()) {
            String literal = constMatcher.group(1);
            return quoteJsltString(literal);
        }

        if (v.startsWith("exp{")) {
            String inner = v.substring(4, v.length() - 1).trim();
            String jsltExpr = parseExpToJslt(inner);
            if (jsltExpr != null && dateColumnOutputKey != null && dateColumnOutputKey.equals(outputKey)
                    && existingDateFormat != null && newDateFormat != null) {
                return buildFormatTimeParseTime(jsltExpr, existingDateFormat, newDateFormat, timezoneId);
            }
            return jsltExpr != null ? jsltExpr : "null";
        }

        String fieldExpr = toJsltSelector(v);
        if (dateColumnOutputKey != null && dateColumnOutputKey.equals(outputKey)
                && existingDateFormat != null && newDateFormat != null) {
            return buildFormatTimeParseTime(fieldExpr, existingDateFormat, newDateFormat, timezoneId);
        }
        return fieldExpr;
    }

    private static String buildFormatTimeParseTime(String expr, String existingDateFormat, String newDateFormat, String timezoneId) {
        String parseCall = "parse-time(" + expr + ", " + quoteJsltString(existingDateFormat)
                + (timezoneId != null && !timezoneId.isEmpty() ? ", " + quoteJsltString(timezoneId) : "") + ")";
        return "format-time(" + parseCall + ", " + quoteJsltString(newDateFormat)
                + (timezoneId != null && !timezoneId.isEmpty() ? ", " + quoteJsltString(timezoneId) : "") + ")";
    }

    private static String parseExpToJslt(String inner) {
        Matcher hdrMatcher = HDR_IN_EXP_PATTERN.matcher(inner);
        if (!hdrMatcher.find()) {
            return null;
        }
        String fieldName = hdrMatcher.group(1).trim();
        String fieldSelector = toJsltSelector(fieldName);

        List<String> literals = new ArrayList<>();
        int idx = 0;
        while (idx < inner.length()) {
            int plus = inner.indexOf('+', idx);
            if (plus < 0) break;
            int quoteStart = inner.indexOf('\'', plus + 1);
            if (quoteStart < 0) break;
            int quoteEnd = inner.indexOf('\'', quoteStart + 1);
            if (quoteEnd < 0) break;
            String part = inner.substring(quoteStart + 1, quoteEnd);
            if (!part.isEmpty()) {
                literals.add(part);
            }
            idx = quoteEnd + 1;
        }

        if (literals.isEmpty()) {
            return fieldSelector;
        }
        String combined = String.join("", literals);
        return fieldSelector + " + " + quoteJsltString(combined);
    }

    private static String quoteJsltString(String s) {
        if (s == null) return "\"\"";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }

    private static String quoteKey(String key) {
        return "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String toJsltSelector(String inputKey) {
        if (isValidJsltIdentifier(inputKey)) {
            return "." + inputKey;
        }
        return "get-key(., " + quoteJsltString(inputKey) + ")";
    }

    private static boolean isValidJsltIdentifier(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (i == 0 && !Character.isLetter(c) && c != '_') return false;
            if (i > 0 && !Character.isLetterOrDigit(c) && c != '_') return false;
        }
        return true;
    }
}
