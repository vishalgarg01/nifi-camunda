package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.client.BlockDefinitionClient;
import com.emigran.nifi.migration.model.Block;
import com.emigran.nifi.migration.model.BlockDefinitionResponse;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.Field;
import com.emigran.nifi.migration.model.BlockDefinitionResponse.NiFiProcessorDefinition;
import com.emigran.nifi.migration.model.BlockDefinitionResponse.NiFiPropertyMapping;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Component
public class FlowXmlSecretResolver {
    private static final Logger log = LoggerFactory.getLogger(FlowXmlSecretResolver.class);

    private final FlowXmlFetcher flowXmlFetcher;
    private final BlockDefinitionClient blockDefinitionClient;
    private final NifiDecryptor decryptor;
    private final Map<Long, BlockDefinitionResponse> blockDefinitionCache = new HashMap<>();

    @Autowired
    public FlowXmlSecretResolver(FlowXmlFetcher flowXmlFetcher,
                                 BlockDefinitionClient blockDefinitionClient,
                                 NifiDecryptor decryptor) {
        this.flowXmlFetcher = flowXmlFetcher;
        this.blockDefinitionClient = blockDefinitionClient;
        this.decryptor = decryptor;
    }

    /**
     * Mutates the given dataflow detail, replacing masked/blank field values with values
     * read from flow.xml (and decrypted when possible).
     */
    public void resolve(DataflowDetail detail, String workspaceName, String workspaceUuid, String dataflowUuid) {
        if (detail == null || detail.getBlocks() == null) {
            return;
        }
        Document doc = flowXmlFetcher.getFlowXml();
        Element dataflowGroup = findProcessGroupById(doc.getDocumentElement(), dataflowUuid);
        if (dataflowGroup == null) {
            log.warn("Dataflow {} not found by uuid {} in flow.xml", detail.getName(), dataflowUuid);
            return;
        }
        for (Block block : detail.getBlocks()) {
            BlockDefinitionResponse def = getDefinition(block.getBlockTypeId());
            if (def == null) {
                continue;
            }
            if (block.getFields() == null) {
                continue;
            }
            for (Field field : block.getFields()) {
                if (!isMasked(field.getValue())) {
                    continue;
                }
                String resolved = resolveFieldValue(dataflowGroup, block, def, field.getName());
                if (resolved != null) {
                    field.setValue(decryptor.decrypt(resolved));
                }
            }
        }
    }

    private BlockDefinitionResponse getDefinition(long blockTypeId) {
        if (blockDefinitionCache.containsKey(blockTypeId)) {
            return blockDefinitionCache.get(blockTypeId);
        }
        try {
            BlockDefinitionResponse def = blockDefinitionClient.getBlockDefinition(blockTypeId);
            blockDefinitionCache.put(blockTypeId, def);
            return def;
        } catch (Exception ex) {
            log.warn("Unable to fetch block definition for {}", blockTypeId, ex);
            return null;
        }
    }

    private boolean isMasked(String value) {
        if (value == null) {
            return true;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed) || "********".equals(trimmed);
    }

    private String resolveFieldValue(Element dataflowGroup, Block block, BlockDefinitionResponse def, String fieldName) {
        if (def.getProcessors() == null) {
            return null;
        }
        Optional<Long> fieldId = def.getUiFields() == null ? Optional.empty()
                : def.getUiFields().stream()
                .filter(f -> fieldName.equalsIgnoreCase(f.getName()))
                .map(f -> f.getId())
                .findFirst();

        if (!fieldId.isPresent()) {
            return null;
        }

        for (NiFiProcessorDefinition processorDef : def.getProcessors()) {
            List<NiFiPropertyMapping> props = processorDef.getProperties();
            if (props == null) {
                continue;
            }
            for (NiFiPropertyMapping mapping : props) {
                if (mapping.getFieldId() == null || !mapping.getFieldId().equals(fieldId.get())) {
                    continue;
                }
                String processorNamePrefix = block.getName() + "_";
                String candidate = findProcessorProperty(dataflowGroup, processorNamePrefix + processorDef.getId(),
                        mapping.getNifiKey());
                if (candidate != null) {
                    return candidate;
                }
            }
        }
        return null;
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

    private String findProcessorProperty(Element processGroup, String processorName, String propertyKey) {
        NodeList processors = processGroup.getElementsByTagName("processor");
        String targetKey = extractPropertyKey(propertyKey);
        for (int i = 0; i < processors.getLength(); i++) {
            Node node = processors.item(i);
            if (!(node instanceof Element)) {
                continue;
            }
            Element processor = (Element) node;
            String name = getChildText(processor, "name");
            if (name == null || !name.startsWith(processorName)) {
                continue;
            }
            NodeList properties = processor.getElementsByTagName("property");
            for (int p = 0; p < properties.getLength(); p++) {
                Node propNode = properties.item(p);
                if (!(propNode instanceof Element) || !propNode.getParentNode().equals(processor)) {
                    continue;
                }
                Element propEl = (Element) propNode;
                String keyName = getChildText(propEl, "name");
                if (keyName != null && keyName.equalsIgnoreCase(targetKey)) {
                    return getChildText(propEl, "value");
                }
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

    private String normalizeName(String name) {
        if (name == null) {
            return null;
        }
        String unescaped = StringEscapeUtils.unescapeXml(name);
        return unescaped == null ? null : unescaped.trim();
    }

    private String extractPropertyKey(String propertyKey) {
        if (propertyKey == null) {
            return null;
        }
        String[] parts = propertyKey.split("\\.");
        return parts.length == 0 ? propertyKey : parts[parts.length - 1];
    }
}
