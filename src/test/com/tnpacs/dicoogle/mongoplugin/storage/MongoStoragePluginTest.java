package com.tnpacs.dicoogle.mongoplugin.storage;

import com.tnpacs.dicoogle.mongoplugin.utils.MongoUri;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.jupiter.api.Test;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

class MongoStoragePluginTest {
    MongoStoragePlugin storagePlugin = new MongoStoragePlugin();
    ClassLoader classLoader = getClass().getClassLoader();

    MongoStoragePluginTest() {
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
    void store() {
        try {
            URI uri = new URI("mongodb://localhost:27017/tnpacs/test");
            MongoUri mUri = new MongoUri(uri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (MongoUri.InvalidMongoUriException e) {
            e.printStackTrace();
        }
    }
}