package com.tnpacs.dicoogle.mongoplugin.queryindex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tnpacs.dicoogle.mongoplugin.utils.Defaults;
import com.tnpacs.dicoogle.mongoplugin.utils.Dictionary;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;
import org.dcm4che2.data.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.datastructs.Report;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;
import pt.ua.dicoogle.sdk.task.Task;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoIndexerPluginTest {
    MongoIndexerPlugin indexerPlugin = new MongoIndexerPlugin();
    ClassLoader classLoader = getClass().getClassLoader();
    MongoCollection<Document> metadataCollection;

    MongoIndexerPluginTest() {
        // set settings
        ConfigurationHolder settings;
        try {
            File settingsFile = new File(classLoader.getResource("settings.xml").getFile());
            settings = new ConfigurationHolder(settingsFile);
        } catch (ConfigurationException e) {
            e.printStackTrace();
            return;
        }
        indexerPlugin.setSettings(settings);
        metadataCollection = MongoUtils.getDatabase().getCollection(Defaults.METADATA_COLLECTION_NAME);
    }

    @BeforeEach
    @AfterEach
    void dropMetadata() {
        metadataCollection.drop();
    }

    @Test
    void index() {
        String sopInstanceUid = "1.2.826.0.1.3680043.2.1545.6.203.7";
        StorageInputStream storageInputStream = createStorageInputStream(sopInstanceUid);

        Task<Report> task = indexerPlugin.index(storageInputStream);
        task.onCompletion(() -> {
            Document doc = metadataCollection.find().first();
            assertEquals(doc.getString(Dictionary.getInstance().getName(Tag.SOPInstanceUID)), sopInstanceUid);
        });
        task.run();
    }

    @Test
    void indexMultiple() {
        List<String> sopInstanceUids = new ArrayList<>();
        sopInstanceUids.add("1.2.826.0.1.3680043.2.1545.6.203.7");
        sopInstanceUids.add("1.2.826.0.1.3680043.2.1545.6.484.9");
        sopInstanceUids.add("1.2.826.0.1.3680043.2.1545.6.890.1");
        List<StorageInputStream> storageInputStreams = sopInstanceUids.stream()
                .map(this::createStorageInputStream)
                .collect(Collectors.toList());

        Task<Report> task = indexerPlugin.index(storageInputStreams);
        task.onCompletion(() -> {
            for (Document doc : metadataCollection.find()) {
                assertTrue(sopInstanceUids.contains(doc.getString(Dictionary.getInstance().getName(Tag.SOPInstanceUID))));
            }
        });
        task.run();
    }

    private StorageInputStream createStorageInputStream(String sopInstanceUid) {
        File dicomFile = new File(classLoader
                .getResource(String.format("dicom/%s.dcm", sopInstanceUid))
                .getFile());
        return new StorageInputStream() {
            @Override
            public URI getURI() {
                return dicomFile.toURI();
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return new FileInputStream(dicomFile);
            }

            @Override
            public long getSize() {
                return dicomFile.length();
            }
        };
    }
}