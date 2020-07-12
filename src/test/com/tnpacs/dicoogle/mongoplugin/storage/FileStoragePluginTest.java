package com.tnpacs.dicoogle.mongoplugin.storage;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.jupiter.api.Test;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

class FileStoragePluginTest {
    FileStoragePlugin storagePlugin = new FileStoragePlugin();
    ClassLoader classLoader = getClass().getClassLoader();

    FileStoragePluginTest() {
        // set settings
        ConfigurationHolder settings;
        try {
            File settingsFile = new File(classLoader.getResource("settings.xml").getFile());
            settings = new ConfigurationHolder(settingsFile);
        } catch (ConfigurationException e) {
            e.printStackTrace();
            return;
        }
        storagePlugin.setSettings(settings);
    }

    @Test
    void at() {
        try {
            Iterable<StorageInputStream> dicomFiles = storagePlugin.at(new URI("file:/D:/BK/TNPACS"));
            for (StorageInputStream file : dicomFiles) {
                System.out.println(file.getURI());
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}