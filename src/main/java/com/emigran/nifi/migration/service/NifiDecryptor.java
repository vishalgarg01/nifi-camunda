package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.config.MigrationProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.apache.nifi.encrypt.StringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class NifiDecryptor {
    private static final Logger log = LoggerFactory.getLogger(NifiDecryptor.class);
    private final MigrationProperties properties;
    private final StringEncryptor stringEncryptor;

    @Autowired
    public NifiDecryptor(MigrationProperties properties,
                         @Qualifier("nifiStringEncryptor") StringEncryptor stringEncryptor) {
        this.properties = properties;
        this.stringEncryptor = stringEncryptor;
    }

    /**
     * Attempts to decrypt using NiFi's AESSensitivePropertyProvider. If the key is
     * not provided or decryption fails, the cipher text is returned as-is.
     */
    public String decrypt(String cipherText) {
        if (cipherText == null || cipherText.trim().isEmpty()) {
            return cipherText;
        }

        String rawCipher = cipherText;
        if (cipherText.startsWith("enc{") && cipherText.endsWith("}")) {
            rawCipher = cipherText.substring(4, cipherText.length() - 1);
        } else {
            return cipherText;
        }

        // Preferred path: use NiFi StringEncryptor bean if available
        try {
            return stringEncryptor.decrypt(rawCipher);
        } catch (Exception ex) {
            log.warn("Failed to decrypt with StringEncryptor, falling back. {}", ex.getMessage());
            return rawCipher;
        }


    }
}
