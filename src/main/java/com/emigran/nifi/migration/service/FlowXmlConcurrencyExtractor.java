package com.emigran.nifi.migration.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;

/**
 * Extracts from flow.xml the list of processors whose concurrency is not 1.
 * For each such processor we need to apply the same concurrency on the new dataflow
 * by resolving which old block it belongs to and then updating the corresponding new block.
 * Uses the OLD dataflow UUID to find the process group in flow.xml.
 */
@Component
public class FlowXmlConcurrencyExtractor {
    private static final Logger log = LoggerFactory.getLogger(FlowXmlConcurrencyExtractor.class);
    public Set<String> httpProcessors = new HashSet<>(Arrays.asList("org.apache.nifi.processors.standard.InvokeHTTP", "com.capillary.foundation.processors.InvokeHttpV2", "com.capillary.foundation.processors.OAuthClientProcessor"));

    private final FlowXmlFetcher flowXmlFetcher;

    @Autowired
    public FlowXmlConcurrencyExtractor(FlowXmlFetcher flowXmlFetcher) {
        this.flowXmlFetcher = flowXmlFetcher;
    }

    /**
     * A processor in flow.xml that has maxConcurrentTasks != 1 and needs its
     * concurrency applied to the corresponding block in the new dataflow.
     */
    public static final class ProcessorConcurrencyInfo {
        private final String processorName;
        private final String processorClass;
        private final int currentConcurrency;

        public ProcessorConcurrencyInfo(String processorName, String processorClass, int currentConcurrency) {
            this.processorName = processorName;
            this.processorClass = processorClass;
            this.currentConcurrency = currentConcurrency;
        }

        public String getProcessorName() {
            return processorName;
        }

        public String getProcessorClass() {
            return processorClass;
        }

        public int getCurrentConcurrency() {
            return currentConcurrency;
        }
    }

    /**
     * Returns all processors in the given dataflow (flow.xml) that have maxConcurrentTasks != 1.
     * For each we will resolve the old block (by processor name) and then update the
     * corresponding new block's concurrency via the glue API.
     *
     * @param oldDataflowUuid process group id (dataflow UUID) in flow.xml
     * @return list of processors with concurrency != 1; empty if group not found
     */
    public List<ProcessorConcurrencyInfo> getProcessorsWithConcurrencyNotOne(String oldDataflowUuid) {
        log.info("[FlowXmlConcurrencyExtractor] getProcessorsWithConcurrencyNotOne START - oldDataflowUuid={}", oldDataflowUuid);
        List<ProcessorConcurrencyInfo> result = new ArrayList<>();
        Document doc = flowXmlFetcher.getFlowXml();
        Element dataflowGroup = findProcessGroupById(doc.getDocumentElement(), oldDataflowUuid);
        if (dataflowGroup == null) {
            log.warn("[FlowXmlConcurrencyExtractor] Dataflow process group not found in flow.xml for uuid={}", oldDataflowUuid);
            return result;
        }

        NodeList processors = dataflowGroup.getElementsByTagName("processor");
        log.debug("[FlowXmlConcurrencyExtractor] Scanning {} processor(s) in process group", processors.getLength());
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
            String processorClass = getChildText(processor, "class");
            int concurrency = parseConcurrency(getChildText(processor, "maxConcurrentTasks"));

            if (concurrency == 1 && !httpProcessors.contains(processorClass) ) {
                continue;
            }
            result.add(new ProcessorConcurrencyInfo(
                    processorName != null ? processorName : "",
                    processorClass != null ? processorClass : "",
                    concurrency));
        }
        log.info("[FlowXmlConcurrencyExtractor] getProcessorsWithConcurrencyNotOne END - found {} processor(s)", result.size());
        return result;
    }

    private static int parseConcurrency(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 1;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 1;
        }
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
