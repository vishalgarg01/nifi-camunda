package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

@Service
public class FlowXmlFetcher {
    private static final Logger log = LoggerFactory.getLogger(FlowXmlFetcher.class);

    private final MigrationProperties properties;

    @Autowired
    public FlowXmlFetcher(MigrationProperties properties) {
        this.properties = properties;
    }

    public Document getFlowXml() {
        try {
            File cached = ensureLocalCopy();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(cached);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to load flow.xml from SFTP", ex);
        }
    }

    private File ensureLocalCopy() throws Exception {
        File dir = new File(properties.getFlowXmlCacheDir());
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Failed to create cache dir " + dir.getAbsolutePath());
        }
        File target = new File(dir, "flow.xml");
        if (target.exists() && target.length() > 0) {
            return target;
        }
        downloadTo(target);
        return target;
    }

    private void downloadTo(File target) throws Exception {
        log.info("Downloading flow.xml from SFTP {}:{}", properties.getSftpHost(), properties.getFlowXmlRemotePath());
        JSch jsch = new JSch();
        Session session = jsch.getSession(properties.getSftpUsername(), properties.getSftpHost(), 22);
        session.setPassword(properties.getSftpPassword());
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect(30_000);
        try {
            ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(30_000);
            try (InputStream inputStream = channel.get(properties.getFlowXmlRemotePath());
                 FileOutputStream out = new FileOutputStream(target)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = inputStream.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            } finally {
                channel.disconnect();
            }
        } finally {
            session.disconnect();
        }
    }
}
