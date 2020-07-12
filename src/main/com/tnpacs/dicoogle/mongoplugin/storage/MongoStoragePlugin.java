package com.tnpacs.dicoogle.mongoplugin.storage;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;
import com.tnpacs.dicoogle.mongoplugin.MongoBasePlugin;
import com.tnpacs.dicoogle.mongoplugin.utils.Constants;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUri;
import com.tnpacs.dicoogle.mongoplugin.utils.MongoUtils;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.data.Tag;
import org.dcm4che2.io.DicomInputStream;
import org.dcm4che2.io.DicomOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.StorageInterface;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class MongoStoragePlugin extends MongoBasePlugin implements StorageInterface {
    private static final Logger logger = LoggerFactory.getLogger(MongoStoragePlugin.class);
    private String authority;

    private GridFSBucket bucket;

    @Override
    public String getName() {
        return "mongo-storage-plugin";
    }

    @Override
    public String getScheme() {
        return Constants.MONGODB_SCHEME;
    }

    @Override
    public Iterable<StorageInputStream> at(URI uri, Object... objects) {
        if (!isEnabled) return null;

        try {
            MongoUri mongoUri = new MongoUri(uri);
            return () -> {
                ArrayList<StorageInputStream> results = new ArrayList<>();

                Bson filter;
                if (mongoUri.isComplete()) {
                    // get one specific file
                    filter = Filters.eq("filename", mongoUri.getFileName());
                } else if (mongoUri.getStudyInstanceUid().isEmpty()) {
                    // find every file in database
                    filter = new BsonDocument();
                } else if (mongoUri.getSeriesInstanceUid().isEmpty()) {
                    // find every file in study
                    String studyInstanceUid = mongoUri.getStudyInstanceUid();
                    filter = Filters.regex("filename", String.format("^%s", studyInstanceUid));
                } else {
                    // find every file in series
                    String studyInstanceUid = mongoUri.getStudyInstanceUid();
                    String seriesInstanceUid = mongoUri.getSeriesInstanceUid();
                    filter = Filters.regex("filename", String.format("^%s/%s", studyInstanceUid, seriesInstanceUid));
                }

                for (GridFSFile file : bucket.find(filter)) {
                    results.add(new StorageInputStream() {
                        @Override
                        public URI getURI() {
                            return uri;
                        }

                        @Override
                        public InputStream getInputStream() {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            bucket.downloadToStream(file.getId(), bos);
                            return new ByteArrayInputStream(bos.toByteArray());
                        }

                        @Override
                        public long getSize() {
                            return file.getLength();
                        }
                    });
                }
                return results.iterator();
            };
        } catch (MongoUri.InvalidMongoUriException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public URI store(DicomObject dicomObject, Object... objects) {
        if (!isEnabled) return null;

        String studyInstanceUid = dicomObject.get(Tag.StudyInstanceUID).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
        String seriesInstanceUid = dicomObject.get(Tag.SeriesInstanceUID).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
        String sopInstanceUid = dicomObject.get(Tag.SOPInstanceUID).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
        String fileName = String.format("%s/%s/%s", studyInstanceUid, seriesInstanceUid, sopInstanceUid);
        MongoUri mongoUri;
        try {
            // example: mongodb://localhost:27017/tnpacs/<StudyInstanceUID>/<SeriesInstanceUID>/<SOPInstanceUID>
            mongoUri = new MongoUri(authority, MongoUtils.getDatabase().getName(),
                    studyInstanceUid, seriesInstanceUid, sopInstanceUid);
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
            return null;
        }

        logger.info("Storing: " + mongoUri);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DicomOutputStream dos = new DicomOutputStream(bos);
            dos.writeDicomFile(dicomObject);

            // find old files
            List<GridFSFile> oldFiles = new ArrayList<>();
            bucket.find(Filters.eq("filename", fileName))
                    .forEach(oldFiles::add);

            // create new file
            GridFSUploadStream uploadStream = bucket.openUploadStream(fileName);
            uploadStream.write(bos.toByteArray());
            uploadStream.close();

            // delete old files
            for (GridFSFile file : oldFiles) {
                logger.info(String.format("Removing old file: %s", fileName));
                bucket.delete(file.getId());
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        logger.info("Stored: " + mongoUri);
        return mongoUri.toURI();
    }

    @Override
    public URI store(DicomInputStream dicomInputStream, Object... objects) throws IOException {
        if (!isEnabled || dicomInputStream == null) {
            return null;
        }

        DicomObject dicomObject = dicomInputStream.readDicomObject();
        dicomInputStream.close();
        return store(dicomObject);
    }

    @Override
    public void remove(URI uri) {
        if (!isEnabled) return;

        try {
            MongoUri mongoUri = new MongoUri(uri);
            if (mongoUri.isComplete()) {
                String fileName = mongoUri.getFileName();
                logger.info("Removing: " + uri);
                bucket.find(Filters.eq("filename", fileName))
                        .forEach(file -> bucket.delete(file.getId()));
                logger.info("Removed: " + uri);
            } else {
                logger.error("Not a complete URI");
            }
        } catch (MongoUri.InvalidMongoUriException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void setSettings(ConfigurationHolder configurationHolder) {
        super.setSettings(configurationHolder);
        bucket = GridFSBuckets.create(MongoUtils.getDatabase());
        authority = MongoUtils.getAuthority();
    }
}
