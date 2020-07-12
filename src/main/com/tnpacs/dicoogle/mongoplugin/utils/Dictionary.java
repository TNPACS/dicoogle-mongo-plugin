package com.tnpacs.dicoogle.mongoplugin.utils;

import org.dcm4che2.data.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Hashtable;

public class Dictionary {
    private static final Logger logger = LoggerFactory.getLogger(Dictionary.class);

    private final Hashtable<String, Integer> nameToTag = new Hashtable<>();
    private final Hashtable<Integer, String> tagToName = new Hashtable<>();

    private static Dictionary instance;

    public static Dictionary getInstance() {
        if (instance == null) instance = new Dictionary();
        return instance;
    }

    private Dictionary() {
        Field[] tags = Tag.class.getFields();
        for (Field tag : tags) {
            try {
                nameToTag.put(tag.getName(), tag.getInt(null));
                tagToName.put(tag.getInt(null), tag.getName());
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public String getName(int tag) {
        return tagToName.get(tag);
    }

    public Integer getTag(String name) {
        return nameToTag.get(name);
    }

    public String[] getNames() {
        return nameToTag.keySet().toArray(new String[0]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String name : nameToTag.keySet()) {
            sb.append(name)
                    .append(" - ")
                    .append(Integer.toHexString(nameToTag.get(name)))
                    .append("\n");
        }
        return sb.toString();
    }
}
