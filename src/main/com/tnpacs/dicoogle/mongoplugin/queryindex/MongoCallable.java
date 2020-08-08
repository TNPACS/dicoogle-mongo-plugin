package com.tnpacs.dicoogle.mongoplugin.queryindex;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.tnpacs.dicoogle.mongoplugin.utils.Constants;
import com.tnpacs.dicoogle.mongoplugin.utils.Dictionary;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dcm4che2.data.DicomElement;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.data.Tag;
import org.dcm4che2.io.DicomInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.datastructs.IndexReport2;
import pt.ua.dicoogle.sdk.datastructs.Report;
import pt.ua.dicoogle.sdk.task.ProgressCallable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MongoCallable implements ProgressCallable<Report> {
    private static final Logger logger = LoggerFactory.getLogger(MongoCallable.class);

    private final Iterable<StorageInputStream> files;
    private final long numFiles;
    private final MongoCollection<Document> collection;
    private final String logFileName;
    private float progress = 0.0f;

    public MongoCallable(Iterable<StorageInputStream> files, long numFiles, MongoCollection<Document> collection, String logFileName) {
        this.files = files;
        this.numFiles = numFiles;
        this.collection = collection;
        this.logFileName = logFileName;
    }

    @Override
    public Report call() {
        if (files == null) return null;
        int numIndexed = 0, numErrors = 0;

        long indexStart = System.currentTimeMillis();
        for (StorageInputStream file : files) {
            try {
                long start, end;

                start = System.currentTimeMillis();

                DicomInputStream dicomInputStream = new DicomInputStream(file.getInputStream());
                DicomObject dicomObject = dicomInputStream.readDicomObject();
                dicomInputStream.close();

                String sopInstanceUid = dicomObject.get(Tag.SOPInstanceUID).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
                // if this instance already has a duplicate, skip it
                if (isSOPInstanceUIDIndexed(sopInstanceUid)) {
                    logger.warn("SOPInstanceUID already indexed: " + sopInstanceUid);
                    numErrors++;
                    continue;
                }

                Map<String, Object> map = retrieveHeaders(dicomObject);
                map.put(Constants.METADATA_URI, file.getURI().toString());
                Document doc = new Document(map);
                collection.insertOne(doc);

                end = System.currentTimeMillis();

                // write to log
                FileWriter fileWriter = new FileWriter(logFileName, true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                bufferedWriter.newLine();
                bufferedWriter.write(String.format("%s %d %d", sopInstanceUid, start, end));
                bufferedWriter.close();
                fileWriter.close();

                logger.info("Indexed: " + file.getURI());
                numIndexed++;
            } catch (ClosedByInterruptException ex) {
                logger.info("Task cancelled: " + file.getURI());
                numErrors++;
            } catch (IOException ex) {
                logger.warn("Indexing failed: " + file.getURI(), ex);
                numErrors++;
            } finally {
                if (numFiles > 0) progress = (numIndexed + numErrors) / (float) numFiles;
            }
        }
        long indexEnd = System.currentTimeMillis();

        progress = 1.0f;
        return new IndexReport2(numIndexed, numErrors, indexEnd - indexStart);
    }

    private Map<String, Object> retrieveHeaders(DicomObject dicomObject) {
        Map<String, Object> map = new HashMap<>();
        if (dicomObject == null) return map;
        Dictionary dict = Dictionary.getInstance();

        // VRMap for storing mapping between tag name and VR
        Map<String, String> vrMap = new HashMap<>();

        for (Iterator<DicomElement> it = dicomObject.datasetIterator(); it.hasNext(); ) {
            DicomElement element = it.next();
            int tag = element.tag();

            // skip pixel data
            if (tag == Tag.PixelData) continue;

            // get tag name and put tag name into VRMap
            String tagName = dict.getName(tag);
            if (tagName == null) tagName = dicomObject.nameOf(tag);
            vrMap.put(tagName, dicomObject.vrOf(tag).toString());

            // sequence
            if (dicomObject.vrOf(tag).toString().equals("SQ") && element.hasItems()) {
                map.putAll(retrieveHeaders(element.getDicomObject()));
                continue;
            }

            try {
                String tagValue = dicomObject.get(tag).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
                map.put(tagName, tagValue);
            } catch (Exception ex) {
                map.put(tagName, null);
            }
        }
        map.put(Constants.METADATA_VR_MAP, vrMap);

        return map;
    }

    private boolean isSOPInstanceUIDIndexed(String sopInstanceUID) {
        MongoCollection<Document> metadataCollection = MongoUtils.getMetadataCollection();
        Bson filter = Filters.eq(Dictionary.getInstance().getName(Tag.SOPInstanceUID), sopInstanceUID);
        return metadataCollection.countDocuments(filter) > 0;
    }

    @Override
    public float getProgress() {
        return progress;
    }
}
