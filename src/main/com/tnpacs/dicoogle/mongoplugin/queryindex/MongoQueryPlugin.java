package com.tnpacs.dicoogle.mongoplugin.queryindex;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.tnpacs.dicoogle.mongoplugin.MongoBasePlugin;
import com.tnpacs.dicoogle.mongoplugin.utils.Constants;
import com.tnpacs.dicoogle.mongoplugin.utils.Dictionary;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ua.dicoogle.sdk.QueryInterface;
import pt.ua.dicoogle.sdk.datastructs.SearchResult;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;
import pt.ua.dicoogle.sdk.utils.QueryException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class MongoQueryPlugin extends MongoBasePlugin implements QueryInterface {
    private static final Logger logger = LoggerFactory.getLogger(MongoQueryPlugin.class);

    private MongoCollection<Document> collection;

    public MongoQueryPlugin() {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    @Override
    public String getName() {
        return "mongo-query-plugin";
    }

    @Override
    public Iterable<SearchResult> query(String s, Object... objects) throws QueryException {
        if (!isEnabled) return null;

        String[] fields = Dictionary.getInstance().getNames();
        try {
            MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new StandardAnalyzer());
            String q = s.replace("\\", "\\\\");
            Query luceneQuery = parser.parse(q);
            Bson mongoQuery = MongoUtils.convertLuceneQueryToMongoQuery(luceneQuery);
            logger.info("Query: " + q);
            logger.info("Mongo filter: " + mongoQuery.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()).toJson());
            return collection.find(mongoQuery).map(this::createSearchResult);
        } catch (ParseException e) {
            logger.error("Error while querying. Returning all results.");
            logger.error("Error details: " + e.getMessage());
            return collection.find().map(this::createSearchResult);
        }
    }

    private SearchResult createSearchResult(Document doc) {
        try {
//            MongoUri uri = new MongoUri(MongoUtils.getConnectionString(),
//                    MongoUtils.getDatabase().getName(),
//                    doc.getString(Dictionary.getInstance().getName(Tag.StudyInstanceUID)),
//                    doc.getString(Dictionary.getInstance().getName(Tag.SeriesInstanceUID)),
//                    doc.getString(Dictionary.getInstance().getName(Tag.SOPInstanceUID)));
            HashMap<String, Object> data = new HashMap<>();
            for (String key : doc.keySet()) {
                Object val = doc.get(key);
                if (val != null) data.put(key, val.toString());
                else data.put(key, null);
            }
            return new SearchResult(new URI(doc.getString(Constants.METADATA_URI)), 0.0, data);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    @Override
    public void setSettings(ConfigurationHolder configurationHolder) {
        super.setSettings(configurationHolder);
        collection = MongoUtils.getMetadataCollection();
    }
}
