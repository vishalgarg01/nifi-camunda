package com.emigran.config;

import org.apache.nifi.encrypt.StringEncryptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EncryptionConfig {

    @Value("${nifi.migration.string.encryptor.algorithm}")
    private String nifiStringEncryptorAlgorithm;

    @Value("${nifi.migration.string.encryptor.provider}")
    private String nifiStringEncryptorProvider;

    @Value("${nifi.migration.string.encryptor.key}")
    private String nifiStringEncryptorKey;

    @Bean(name = "nifiStringEncryptor")
    public StringEncryptor stringEncryptor() {
        return new StringEncryptor(nifiStringEncryptorAlgorithm, nifiStringEncryptorProvider, nifiStringEncryptorKey);
    }
}
