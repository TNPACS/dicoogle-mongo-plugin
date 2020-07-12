package com.tnpacs.dicoogle.mongoplugin;

import com.tnpacs.dicoogle.mongoplugin.queryindex.MongoIndexerPlugin;
import com.tnpacs.dicoogle.mongoplugin.queryindex.MongoQueryPlugin;
import com.tnpacs.dicoogle.mongoplugin.storage.FileStoragePlugin;
import com.tnpacs.dicoogle.mongoplugin.storage.MongoStoragePlugin;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ua.dicoogle.sdk.IndexerInterface;
import pt.ua.dicoogle.sdk.PluginSet;
import pt.ua.dicoogle.sdk.QueryInterface;
import pt.ua.dicoogle.sdk.StorageInterface;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@PluginImplementation
public class MongoPluginSet implements PluginSet {
    private static final Logger logger = LoggerFactory.getLogger(MongoPluginSet.class);
    private final List<IndexerInterface> indexPlugins = new ArrayList<>();
    private final List<QueryInterface> queryPlugins = new ArrayList<>();
    private final List<StorageInterface> storagePlugins = new ArrayList<>();

    public MongoPluginSet() {
        logger.info("Initializing Mongo Plugin Set");
        indexPlugins.add(new MongoIndexerPlugin());
        queryPlugins.add(new MongoQueryPlugin());
        storagePlugins.add(new MongoStoragePlugin());
        storagePlugins.add(new FileStoragePlugin());
    }

    @Override
    public String getName() {
        return "mongo-plugin";
    }

    @Override
    public Collection<? extends IndexerInterface> getIndexPlugins() {
        return indexPlugins;
    }

    @Override
    public Collection<? extends QueryInterface> getQueryPlugins() {
        return queryPlugins;
    }

    @Override
    public Collection<? extends StorageInterface> getStoragePlugins() {
        return storagePlugins;
    }

    @Override
    public void setSettings(ConfigurationHolder configurationHolder) {
        if (!MongoUtils.isInitialized()) MongoUtils.initialize(configurationHolder);
    }

    @Override
    public ConfigurationHolder getSettings() {
        return null;
    }

    @Override
    public void shutdown() {
        if (MongoUtils.isInitialized()) MongoUtils.destroy();
    }
}
