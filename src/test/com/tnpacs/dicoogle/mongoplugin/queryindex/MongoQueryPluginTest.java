package com.tnpacs.dicoogle.mongoplugin.queryindex;

import com.mongodb.client.MongoCollection;
import com.tnpacs.dicoogle.mongoplugin.utils.Defaults;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import pt.ua.dicoogle.sdk.datastructs.SearchResult;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class MongoQueryPluginTest {
    MongoQueryPlugin queryPlugin = new MongoQueryPlugin();
    ClassLoader classLoader = getClass().getClassLoader();
    MongoCollection<Document> metadataCollection;

    MongoQueryPluginTest() {
        // set settings
        ConfigurationHolder settings;
        try {
            File settingsFile = new File(classLoader.getResource("settings.xml").getFile());
            settings = new ConfigurationHolder(settingsFile);
        } catch (ConfigurationException e) {
            e.printStackTrace();
            return;
        }
        queryPlugin.setSettings(settings);
        metadataCollection = MongoUtils.getDatabase().getCollection(Defaults.METADATA_COLLECTION_NAME);
    }

    @Test
    void query() {
        Iterable<SearchResult> results = queryPlugin.query(
                "StudyInstanceUID:100.118.116.2005.2.1.1132565441.485.3" +
                        "\\100.118.116.2005.2.1.1143729853.921.5" +
                        "\\1.3.46.670589.11.42053.5.0.4816.2014031216311512355");
        for (SearchResult result : results) {
            if (result != null) System.out.println(result.getExtraData());
        }
    }
}