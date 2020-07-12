package com.tnpacs.dicoogle.mongoplugin.queryindex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.tnpacs.dicoogle.mongoplugin.MongoBasePlugin;
import com.tnpacs.dicoogle.mongoplugin.utils.*;
import org.bson.Document;
import org.dcm4che2.data.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ua.dicoogle.sdk.IndexerInterface;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.datastructs.Report;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;
import pt.ua.dicoogle.sdk.task.Task;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;

public class MongoIndexerPlugin extends MongoBasePlugin implements IndexerInterface {
    private static final Logger logger = LoggerFactory.getLogger(MongoIndexerPlugin.class);

    private static final String logFileName = "mongo-indexer.log.txt";

    private MongoCollection<Document> metadataCollection;

    @Override
    public String getName() {
        return "mongo-indexer-plugin";
    }

    @Override
    public Task<Report> index(StorageInputStream storageInputStream, Object... objects) {
        if (!isEnabled) return null;
        List<StorageInputStream> inputStreamList = new ArrayList<>(1);
        inputStreamList.add(storageInputStream);
        MongoCallable callable = new MongoCallable(inputStreamList, 1, metadataCollection, logFileName);
        return new Task<>(callable);
    }

    @Override
    public Task<Report> index(Iterable<StorageInputStream> files, Object... objects) {
        if (!isEnabled) return null;
        long numFiles;
        if (files instanceof Collection) numFiles = ((Collection<?>) files).size();
        else numFiles = StreamSupport.stream(files.spliterator(), false).count();
        MongoCallable callable = new MongoCallable(files, numFiles, metadataCollection, logFileName);
        return new Task<>(callable);
    }

    @Override
    public boolean unindex(URI uri) {
        if (!isEnabled) return false;
        try {
            MongoUri mongoUri = new MongoUri(uri);
            if (mongoUri.isComplete()) {
                metadataCollection.findOneAndDelete(Filters.eq(
                        Dictionary.getInstance().getName(Tag.SOPInstanceUID),
                        mongoUri.getSopInstanceUid()));
                logger.info("Unindexed: " + uri);
                return true;
            } else {
                logger.error("Not a complete URI");
                return false;
            }
        } catch (MongoUri.InvalidMongoUriException e) {
            logger.error("Unindexing error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void setSettings(ConfigurationHolder configurationHolder) {
        super.setSettings(configurationHolder);
        metadataCollection = MongoUtils.getMetadataCollection();
    }
}
