package com.tnpacs.dicoogle.mongoplugin;

import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import pt.ua.dicoogle.sdk.DicooglePlugin;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

public abstract class MongoBasePlugin implements DicooglePlugin {
    protected ConfigurationHolder settings;
    protected boolean isEnabled = true;

    @Override
    public abstract String getName();

    @Override
    public boolean enable() {
        isEnabled = true;
        return true;
    }

    @Override
    public boolean disable() {
        isEnabled = false;
        return true;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public void setSettings(ConfigurationHolder configurationHolder) {
        settings = configurationHolder;
        if (!MongoUtils.isInitialized()) MongoUtils.initialize(settings);
    }

    @Override
    public ConfigurationHolder getSettings() {
        return settings;
    }
}
