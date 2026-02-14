package com.emigran.nifi.migration.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MigrationProperties {

    @Value("${nifi.migration.old.base-url}")
    private String oldBaseUrl;

    @Value("${nifi.migration.new.base-url}")
    private String newBaseUrl;

    @Value("${nifi.migration.auth.basic}")
    private String basicAuthToken;

    @Value("${nifi.migration.source-header:migration}")
    private String sourceHeader;

    @Value("${nifi.migration.flow-xml.sftp.host}")
    private String sftpHost;

    @Value("${nifi.migration.flow-xml.sftp.username}")
    private String sftpUsername;

    @Value("${nifi.migration.flow-xml.sftp.password}")
    private String sftpPassword;

    @Value("${nifi.migration.flow-xml.remote-path}")
    private String flowXmlRemotePath;

    @Value("${nifi.migration.flow-xml.cache-dir:/tmp/nifi-flow-cache}")
    private String flowXmlCacheDir;

    @Value("${nifi.migration.log-dir:/tmp/nifi-migration-logs}")
    private String logDir;

    @Value("${nifi.migration.glue.block-definition-url}")
    private String glueBlockDefinitionUrl;

    @Value("${nifi.migration.flow-xml.sensitive-key:}")
    private String sensitiveKey;

    @Value("${nifi.migration.neo-rule.base-url:}")
    private String neoRuleBaseUrl;

    @Value("${nifi.migration.neo-rule.application-id:}")
    private String neoRuleApplicationId;

    @Value("${nifi.migration.neo-rule.context:connectplus}")
    private String neoRuleContext;

    @Value("${nifi.migration.neo-rule.cookie:}")
    private String neoRuleCookie;

    @Value("${nifi.migration.neo-rule.remote-user:}")
    private String neoRuleRemoteUser;

    public String getOldBaseUrl() {
        return oldBaseUrl;
    }

    public String getNewBaseUrl() {
        return newBaseUrl;
    }

    public String getBasicAuthToken() {
        return basicAuthToken;
    }

    public String getSourceHeader() {
        return sourceHeader;
    }

    public String getSftpHost() {
        return sftpHost;
    }

    public String getSftpUsername() {
        return sftpUsername;
    }

    public String getSftpPassword() {
        return sftpPassword;
    }

    public String getFlowXmlRemotePath() {
        return flowXmlRemotePath;
    }

    public String getFlowXmlCacheDir() {
        return flowXmlCacheDir;
    }

    public String getLogDir() {
        return logDir;
    }

    public String getGlueBlockDefinitionUrl() {
        return glueBlockDefinitionUrl;
    }

    public String getSensitiveKey() {
        return sensitiveKey;
    }

    public String getNeoRuleBaseUrl() {
        return neoRuleBaseUrl;
    }

    public String getNeoRuleApplicationId() {
        return neoRuleApplicationId;
    }

    public String getNeoRuleContext() {
        return neoRuleContext;
    }

    public String getNeoRuleCookie() {
        return neoRuleCookie;
    }

    public String getNeoRuleRemoteUser() {
        return neoRuleRemoteUser;
    }
}
