package com.tnpacs.dicoogle.mongoplugin.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.apache.lucene.search.Query;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class MongoUtils {
    private static boolean isInitialized = false;
    private static MongoClient client = null;
    private static MongoDatabase db = null;
    private static MongoCollection<Document> metadataCollection = null;

    private static String connectionString;

    public static void initialize(ConfigurationHolder settings) {
        connectionString = settings.getConfiguration().getString(Constants.CONNECTION_STRING_KEY, Defaults.CONNECTION_STRING);
        String dbName = settings.getConfiguration().getString(Constants.DATABASE_NAME_KEY, Defaults.DATABASE_NAME);
        String metadataCollectionName = settings.getConfiguration().getString(Constants.METADATA_COLLECTION_NAME_KEY, Defaults.METADATA_COLLECTION_NAME);

        if (connectionString.endsWith("/"))
            connectionString = connectionString.substring(0, connectionString.length() - 1);

        client = MongoClients.create(connectionString);
        db = client.getDatabase(dbName);
        metadataCollection = db.getCollection(metadataCollectionName);
        createIndexes(settings);
        isInitialized = true;
    }

    public static void destroy() {
        client.close();
        db = null;
        metadataCollection = null;
        isInitialized = false;
    }

    public static boolean isInitialized() {
        return isInitialized;
    }

    public static String getConnectionString() {
        if (!isInitialized) throw new RuntimeException("MongoUtils not initialized");
        return connectionString;
    }

    public static String getAuthority() {
        if (!isInitialized) throw new RuntimeException("MongoUtils not initialized");
        try {
            URI uri = new URI(connectionString);
            return uri.getAuthority();
        } catch (URISyntaxException e) {
            return null;
        }
    }

    public static MongoClient getClient() {
        if (!isInitialized) throw new RuntimeException("MongoUtils not initialized");
        return client;
    }

    public static MongoDatabase getDatabase() {
        if (!isInitialized) throw new RuntimeException("MongoUtils not initialized");
        return db;
    }

    public static MongoCollection<Document> getMetadataCollection() {
        if (!isInitialized) throw new RuntimeException("MongoUtils not initialized");
        return metadataCollection;
    }

    public static Bson convertLuceneQueryToMongoQuery(Query query) {
        LuceneQueryToMongoQueryConverter converter = new LuceneQueryToMongoQueryConverter(query);
        return converter.convert();
    }

    private static void createIndexes(ConfigurationHolder settings) {
        NodeList indexes = settings.getConfiguration().getDocument().getElementsByTagName(Constants.INDEXES_KEY)
                .item(0).getChildNodes();
        for (int i = 0; i < indexes.getLength(); i++) {
            if (indexes.item(i) == null || indexes.item(i).getNodeType() == Node.TEXT_NODE) continue;
            Node index = indexes.item(i);
            String indexType = index.getNodeName();
            String order = getAttributeValue(index, Constants.ORDER_ATTRIBUTE_NAME);
            if (indexType.equals(Constants.INDEX_KEY)) {
                if (order != null && order.equals(Constants.ORDER_ATTRIBUTE_DESCENDING)) {
                    metadataCollection.createIndex(Indexes.descending(index.getTextContent()));
                } else {
                    metadataCollection.createIndex(Indexes.ascending(index.getTextContent()));
                }
            } else if (indexType.equals(Constants.TEXT_INDEX_KEY)) {
                metadataCollection.createIndex(Indexes.text(index.getTextContent()));
            } else if (indexType.equals(Constants.COMPOUND_INDEX_KEY)) {
                NodeList fields = index.getOwnerDocument().getElementsByTagName(Constants.FIELD_KEY);
                ArrayList<Bson> fieldIndexes = new ArrayList<>();
                for (int j = 0; j < indexes.getLength(); j++) {
                    if (fields.item(j) == null || fields.item(j).getNodeType() == Node.TEXT_NODE) continue;
                    Node field = fields.item(j);
                    String fieldOrder = getAttributeValue(field, Constants.ORDER_ATTRIBUTE_NAME);
                    if (fieldOrder == null) fieldOrder = order;
                    if (fieldOrder != null && fieldOrder.equals(Constants.ORDER_ATTRIBUTE_DESCENDING)) {
                        fieldIndexes.add(Indexes.descending(field.getTextContent()));
                    } else {
                        fieldIndexes.add(Indexes.ascending(field.getTextContent()));
                    }
                }
                metadataCollection.createIndex(Indexes.compoundIndex(fieldIndexes));
            }
        }
    }

    private static String getAttributeValue(Node node, String attributeName) {
        Node attribute = node.getAttributes().getNamedItem(attributeName);
        if (attribute == null) return null;
        return attribute.getNodeValue();
    }
}
