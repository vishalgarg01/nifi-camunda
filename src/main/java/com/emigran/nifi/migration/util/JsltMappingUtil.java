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
    private static final Pattern CONCAT_PREFIX_PATTERN = Pattern.compile("^\\s*'([^']+)'\\s*\\.concat\\s*\\(");
    private static final Pattern BASE64_PATTERN = Pattern.compile("^base64\\{([^}]+)\\}$");
    private static final Pattern BASE64_HDR_IN_EXP_PATTERN = Pattern.compile("base64\\{hdr\\{([^}]+)\\}\\}");

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
     * Resolves a JSONPath expression from an EvaluateJsonPath property (e.g. "$.['attribution.store.identifierValue']")
     * to the source column name using the header mapping.
     * The mapping key is the output/mapped name; the value is the source CSV column name.
     *
     * @param jsonPathValue   JSONPath expression or plain field name from flow.xml
     * @param headerMappingJson JSON header mapping (output key → source/input value)
     * @return source column name if found and is a simple field; stripped field name if not in mapping; null if blank
     */
    public static String resolveJsonPathFieldToSourceName(String jsonPathValue, String headerMappingJson) throws IOException {
        if (jsonPathValue == null || jsonPathValue.trim().isEmpty()) return null;
        String fieldName = stripJsonPath(jsonPathValue.trim());
        if (fieldName == null || fieldName.isEmpty()) return null;
        Map<String, String> mapping = OBJECT_MAPPER.readValue(
                headerMappingJson,
                new TypeReference<LinkedHashMap<String, String>>() { }
        );
        String value = mapping.get(fieldName);
        if (value == null) {
            return wrapAsJsonPath(fieldName);
        }
        if (!isSimpleFieldName(value)) {
            return extractConstantValue(value);
        }
        return wrapAsJsonPath(value);
    }

    /**
     * Strips JSONPath bracket/dot notation to extract the bare field name.
     * E.g. "$.['field.name']" → "field.name", "$.field" → "field", "field" → "field".
     */
    /**
     * Extracts the inner value from expressions like "const{TILL}", "exp{...}", etc.
     * Returns the value between { and }, or the original string if no braces found.
     */
    private static String extractConstantValue(String value) {
        if (value == null) return null;
        int open = value.indexOf('{');
        int close = value.lastIndexOf('}');
        if (open >= 0 && close > open) {
            return value.substring(open + 1, close);
        }
        return value;
    }

    private static String wrapAsJsonPath(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) return fieldName;
        return "$['" + fieldName + "']";
    }

    private static String stripJsonPath(String s) {
        // Handle $[0].['field.name'] or $[0].["field.name"] — strip array index prefix first
        if (s.matches("^\\$\\[\\d+\\]\\..*")) {
            s = "$" + s.substring(s.indexOf("].") + 1); // e.g. $.['field.name']
        }
        if (s.startsWith("$.[")) {
            String after = s.substring(3); // e.g. "'field.name']"
            if (after.startsWith("'") && after.endsWith("']")) {
                return after.substring(1, after.length() - 2);
            }
            if (after.startsWith("\"") && after.endsWith("\"]")) {
                return after.substring(1, after.length() - 2);
            }
            if (after.endsWith("]")) {
                return after.substring(0, after.length() - 1);
            }
        }
        if (s.startsWith("$.")) {
            return s.substring(2);
        }
        return s;
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
        String innerTransform = sb.toString();
        // When input is an array (e.g. [{ "First Name": "...", "Last Name": "..." }]), apply transform to each element.
        // When input is a single object, apply transform as-is.
        // NiFi JSLT does not support type(); use is-array(.) per JSLT built-in functions.
        return "if (is-array(.)) [ for (.) " + innerTransform + " ] else " + innerTransform;
    }

    static String parseValueToJslt(String value, String outputKey,
                                   String dateColumnOutputKey, String existingDateFormat, String newDateFormat,
                                   String timezoneId) {
        if (value == null || value.trim().isEmpty()) {
            return "null";
        }
        String v = value.trim();
        // Strip surrounding double-quotes if the value was escaped in JSON source
        // e.g. "\"exp{...}\"" parses to "exp{...}" (with literal surrounding double-quotes)
        if (v.startsWith("\"") && v.endsWith("\"") && v.length() >= 2) {
            v = v.substring(1, v.length() - 1).trim();
        }

        Matcher constMatcher = CONST_PATTERN.matcher(v);
        if (constMatcher.matches()) {
            String literal = constMatcher.group(1);
            return quoteJsltString(literal);
        }

        Matcher base64Matcher = BASE64_PATTERN.matcher(v);
        if (base64Matcher.matches()) {
            String fieldName = base64Matcher.group(1).trim();
            return quoteJsltString("${" + fieldName + ":base64Encode()}");
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
        // If the format expects a time component (contains HH), add a fallback that appends
        // a default time when the input value lacks one (detected by absence of ':')
        String effectiveExpr = expr;
        if (existingDateFormat != null && existingDateFormat.contains("HH")) {
            String defaultTimeSuffix = buildDefaultTimeSuffix(existingDateFormat);
            if (defaultTimeSuffix != null) {
                effectiveExpr = "if (contains(" + expr + ", \":\")) " + expr
                        + " else " + expr + " + " + quoteJsltString(defaultTimeSuffix);
            }
        }
        String parseCall = "parse-time(" + effectiveExpr + ", " + quoteJsltString(existingDateFormat)
                + (timezoneId != null && !timezoneId.isEmpty() ? ", " + quoteJsltString(timezoneId) : "") + ")";
        return "format-time(" + parseCall + ", " + quoteJsltString(newDateFormat)
                + (timezoneId != null && !timezoneId.isEmpty() ? ", " + quoteJsltString(timezoneId) : "") + ")";
    }

    /**
     * Builds the default time suffix to append when input is date-only.
     * E.g., for format "dd-MM-yyyy HH:mm:ss" returns " 00:00:00",
     *        for format "dd-MM-yyyy'T'HH:mm:ss" returns "T00:00:00".
     */
    private static String buildDefaultTimeSuffix(String dateFormat) {
        int hhIndex = dateFormat.indexOf("HH");
        if (hhIndex <= 0) return null;
        int sepStart = hhIndex - 1;
        // If the char before HH is a closing quote, find the opening quote to include the literal
        if (dateFormat.charAt(sepStart) == '\'') {
            int openQuote = dateFormat.lastIndexOf('\'', sepStart - 1);
            if (openQuote >= 0) {
                sepStart = openQuote;
            }
        }
        String separatorAndTimeFmt = dateFormat.substring(sepStart);
        // Strip single quotes used for literal characters in date format patterns
        separatorAndTimeFmt = separatorAndTimeFmt.replace("'", "");
        return separatorAndTimeFmt
                .replace("HH", "00")
                .replace("mm", "00")
                .replace("ss", "00")
                .replace("SSS", "000");
    }

    private static String parseExpToJslt(String inner) {
        // Handle 'prefix'.concat(base64{hdr{field}}) — base64 encode with optional prefix
        Matcher base64HdrMatcher = BASE64_HDR_IN_EXP_PATTERN.matcher(inner);
        if (base64HdrMatcher.find()) {
            String fieldName = base64HdrMatcher.group(1).trim();
            String nifiEl = "${" + fieldName + ":base64Encode()}";
            Matcher prefixMatcher = CONCAT_PREFIX_PATTERN.matcher(inner);
            if (prefixMatcher.find()) {
                String prefix = prefixMatcher.group(1);
                return quoteJsltString(prefix + nifiEl);
            }
            return quoteJsltString(nifiEl);
        }

        Matcher hdrMatcher = HDR_IN_EXP_PATTERN.matcher(inner);
        if (!hdrMatcher.find()) {
            return null;
        }
        String fieldName = hdrMatcher.group(1).trim();
        String fieldSelector = toJsltSelector(fieldName);

        // Handle 'prefix'.concat(...hdr{field}...) pattern — prefix literal comes before the field
        Matcher prefixMatcher = CONCAT_PREFIX_PATTERN.matcher(inner);
        if (prefixMatcher.find()) {
            String prefix = prefixMatcher.group(1);
            return quoteJsltString(prefix) + " + " + fieldSelector;
        }

        // Handle hdr{field} + 'literal' pattern — literal comes after the field
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
