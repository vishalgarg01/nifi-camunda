package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.model.TransformFlowProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;

/**
 * Extracts properties from flow.xml for transform-type dataflows.
 * Looks for HeaderProcessor (Rename Headers Mapping, date fields), GroupByWithLimit/AggregatorProcessor
 * (Record Group By), and JoltTransformJSON (Jolt Specification) by scanning processors in the dataflow's
 * process group.
 */
@Component
public class FlowXmlTransformPropertiesExtractor {
    private static final Logger log = LoggerFactory.getLogger(FlowXmlTransformPropertiesExtractor.class);

    /** NiFi property names for HeaderProcessor (from flow.xml). */
    private static final String RENAME_HEADERS_MAPPING = "Rename Headers Mapping";
    private static final String DATE_COLUMN_HEADER = "Date Column Header";
    private static final String EXISTING_DATE_FORMAT = "Existing Date Format";
    private static final String NEW_DATE_FORMAT = "New Date Format";

    private static final String RECORD_GROUP_BY = "Record Group By";
    private static final String MINIMUM_GROUP_RECORD = "Minimum Group Record";
    private static final String RECORDS_PER_SPLIT = "Records Per Split";

    private static final String SORT_HEADERS = "Sort Headers";
    private static final String USE_ALPHABETICAL_SORT = "Use Alphabetical Sort";

    /** Standard NiFi JoltTransformJSON property. */
    private static final String JOLT_SPECIFICATION = "jolt-spec";

    /** EvaluateJsonPath / attribution fields (property names in flow.xml; may be snake_case or camelCase). */
    private static final String[] ATTRIBUTION_TYPE_KEYS = {"attribution_type", "attributionType"};
    private static final String[] ATTRIBUTION_CODE_KEYS = {"attribution_code", "attributionCode"};
    private static final String[] HEADER_VALUE_KEYS = {"header_value", "headerValue"};
    private static final String[] CHILD_TILL_CODE_KEYS = {"child_till_code", "childTillCode"};
    private static final String[] CHILD_ORG_ID_KEYS = {"child_org_id", "childOrgId"};

    /** When choosing among multiple "Records Per Split" values, take one with numeric value less than this. */
    private static final int RECORDS_PER_SPLIT_MAX = 100;
    private static final String LINE_NO = "lineNos";

    private final FlowXmlFetcher flowXmlFetcher;

    @Autowired
    public FlowXmlTransformPropertiesExtractor(FlowXmlFetcher flowXmlFetcher) {
        this.flowXmlFetcher = flowXmlFetcher;
    }

    /**
     * Extracts transform-related properties from flow.xml for the given dataflow UUID.
     * Does not filter by processor name.
     */
    public TransformFlowProperties extract(String dataflowUuid) {
        return extract(dataflowUuid, null);
    }

    /**
     * Extracts transform-related properties from flow.xml for the given dataflow UUID.
     * When {@code blockNamePrefix} is non-null, only processors whose name starts with that prefix are considered
     * (e.g. transform block name "Transform-data_7" so that only processors belonging to that block are used).
     *
     * @param dataflowUuid    process group id (dataflow UUID) in flow.xml
     * @param blockNamePrefix optional; when set, processor name must start with this prefix
     * @return extracted properties, or empty object if group not found or no matching processors
     */
    public TransformFlowProperties extract(String dataflowUuid, String blockNamePrefix) {
        TransformFlowProperties out = new TransformFlowProperties();
        Document doc = flowXmlFetcher.getFlowXml();
        Element dataflowGroup = findProcessGroupById(doc.getDocumentElement(), dataflowUuid);
        if (dataflowGroup == null) {
            log.warn("Dataflow process group not found in flow.xml for uuid={}", dataflowUuid);
            return out;
        }

        String recordsPerSplitLessThan100 = null;

        NodeList processors = dataflowGroup.getElementsByTagName("processor");
        for (int i = 0; i < processors.getLength(); i++) {
            Node node = processors.item(i);
            if (!(node instanceof Element)) {
                continue;
            }
            Element processor = (Element) node;
            if (!isDescendantOf(processor, dataflowGroup)) {
                continue;
            }
            String processorName = getChildText(processor, "name");
            if (blockNamePrefix != null && !blockNamePrefix.isEmpty()
                    && (processorName == null || !processorName.startsWith(blockNamePrefix))) {
                continue;
            }
            Map<String, String> props = getProcessorProperties(processor);
            if (props.containsKey(RENAME_HEADERS_MAPPING)) {
                applyHeaderProcessorProps(props, out);
            }
            if (props.containsKey(RECORD_GROUP_BY)) {
                String val = props.get(RECORD_GROUP_BY);
                if (val != null && !val.trim().isEmpty()) {
                    out.setRecordGroupBy(val.trim());
                }
            }
            if (props.containsKey(MINIMUM_GROUP_RECORD)) {
                String val = props.get(MINIMUM_GROUP_RECORD);
                if (val != null && !val.trim().isEmpty()) {
                    out.setGroupSize(val.trim());
                }
            }
            if (props.containsKey(RECORDS_PER_SPLIT) && recordsPerSplitLessThan100 == null) {
                String val = props.get(RECORDS_PER_SPLIT);
                if (val != null && !val.trim().isEmpty()) {
                    int num = parseIntSafe(val.trim(), -1);
                    if (num >= 0 && num < RECORDS_PER_SPLIT_MAX) {
                        recordsPerSplitLessThan100 = val.trim();
                    }
                }
            }
            if (props.containsKey(SORT_HEADERS)) {
                String val = props.get(SORT_HEADERS);
                if (val != null && !val.trim().isEmpty()) {
                    out.setSortHeaders(val.trim());
                }
            }
            if (props.containsKey(USE_ALPHABETICAL_SORT)) {
                String val = props.get(USE_ALPHABETICAL_SORT);
                if (val != null && !val.trim().isEmpty()) {
                    out.setAlphabeticalSort(val.trim());
                }
            }
            if (props.containsKey(JOLT_SPECIFICATION)) {
                String val = props.get(JOLT_SPECIFICATION);
                if (val != null && !val.trim().isEmpty()) {
                    out.setJoltSpec(val.trim());
                }
            }
            if (props.containsKey(LINE_NO)) {
                String val = props.get(LINE_NO);
                if (val != null && !val.trim().isEmpty()) {
                    out.setLineNo(val.trim());
                }
            }
            applyAttributionProps(props, out);
        }
        if (out.getGroupSize() == null && recordsPerSplitLessThan100 != null) {
            out.setGroupSize(recordsPerSplitLessThan100);
        }
        return out;
    }

    private static int parseIntSafe(String s, int defaultValue) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void applyAttributionProps(Map<String, String> props, TransformFlowProperties out) {
        setIfPresentAnyKey(props, ATTRIBUTION_TYPE_KEYS, out::setAttributionType);
        setIfPresentAnyKey(props, ATTRIBUTION_CODE_KEYS, out::setAttributionCode);
        setIfPresentAnyKey(props, HEADER_VALUE_KEYS, out::setHeaderValue);
        setIfPresentAnyKey(props, CHILD_TILL_CODE_KEYS, out::setChildTillCode);
        setIfPresentAnyKey(props, CHILD_ORG_ID_KEYS, out::setChildOrgId);
    }

    private void setIfPresentAnyKey(Map<String, String> props, String[] keys, java.util.function.Consumer<String> setter) {
        for (String key : keys) {
            String val = props.get(key);
            if (val != null && !val.trim().isEmpty()) {
                setter.accept(val.trim());
                return;
            }
        }
    }

    private void applyHeaderProcessorProps(Map<String, String> props, TransformFlowProperties out) {
        String mapping = props.get(RENAME_HEADERS_MAPPING);
        if (mapping != null && !mapping.trim().isEmpty()) {
            out.setHeaderMappingJson(mapping.trim());
        }
        String dateHeader = props.get(DATE_COLUMN_HEADER);
        if (dateHeader != null && !dateHeader.trim().isEmpty()) {
            out.setDateColumnOutputKey(dateHeader.trim());
        }
        String existingFmt = props.get(EXISTING_DATE_FORMAT);
        if (existingFmt != null && !existingFmt.trim().isEmpty()) {
            out.setExistingDateFormat(existingFmt.trim());
        }
        String newFmt = props.get(NEW_DATE_FORMAT);
        if (newFmt != null && !newFmt.trim().isEmpty()) {
            out.setNewDateFormat(newFmt.trim());
        }
    }

    private Map<String, String> getProcessorProperties(Element processor) {
        Map<String, String> map = new HashMap<>();
        NodeList propList = processor.getElementsByTagName("property");
        for (int i = 0; i < propList.getLength(); i++) {
            Node n = propList.item(i);
            if (!(n instanceof Element)) {
                continue;
            }
            Element el = (Element) n;
            if (!isDescendantOf(el, processor)) {
                continue;
            }
            String name = getChildText(el, "name");
            String value = getChildText(el, "value");
            if (name != null) {
                map.put(name.trim(), value != null ? value : "");
            }
        }
        return map;
    }

    private boolean isDescendantOf(Element el, Element ancestor) {
        Node p = el.getParentNode();
        while (p != null) {
            if (p.equals(ancestor)) {
                return true;
            }
            p = p.getParentNode();
        }
        return false;
    }

    private Element findProcessGroupById(Element start, String targetId) {
        if (targetId == null) {
            return null;
        }
        NodeList groups = start.getElementsByTagName("processGroup");
        for (int i = 0; i < groups.getLength(); i++) {
            Node node = groups.item(i);
            if (!(node instanceof Element)) {
                continue;
            }
            Element el = (Element) node;
            String id = getChildText(el, "id");
            if (targetId.equals(id)) {
                return el;
            }
        }
        return null;
    }

    private String getChildText(Element parent, String tagName) {
        Element child = getChild(parent, tagName);
        if (child != null && child.getFirstChild() != null) {
            return child.getFirstChild().getNodeValue();
        }
        return null;
    }

    private Element getChild(Element parent, String tagName) {
        NodeList list = parent.getElementsByTagName(tagName);
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node.getParentNode().equals(parent) && node instanceof Element) {
                return (Element) node;
            }
        }
        return null;
    }
}
