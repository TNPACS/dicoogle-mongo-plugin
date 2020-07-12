package com.tnpacs.dicoogle.mongoplugin.utils;

import java.net.URI;
import java.net.URISyntaxException;

public class MongoUri {
    private final URI uri;
    private final boolean isComplete;
    private final String authority;
    private final String dbName;
    private final String studyInstanceUid;
    private final String seriesInstanceUid;
    private final String sopInstanceUid;

    public MongoUri(URI uri) throws InvalidMongoUriException {
        String scheme = uri.getScheme();
        if (!scheme.equals(Constants.MONGODB_SCHEME)) throw new InvalidMongoUriException("Scheme must be mongodb");
        authority = uri.getAuthority();

        String path = uri.getPath();
        String[] parts = path.split("/");

        if (parts.length > 5) throw new InvalidMongoUriException("Incorrect URI format");
        else isComplete = parts.length == 5;

        this.uri = uri;
        dbName = parts.length > 1 ? parts[1] : "";
        studyInstanceUid = parts.length > 2 ? parts[2] : "";
        seriesInstanceUid = parts.length > 3 ? parts[3] : "";
        sopInstanceUid = parts.length > 4 ? parts[4] : "";
    }

    public MongoUri(String authority, String dbName, String studyInstanceUid, String seriesInstanceUid, String sopInstanceUid) throws URISyntaxException {
        this.authority = authority;
        this.dbName = dbName;
        this.studyInstanceUid = studyInstanceUid;
        this.seriesInstanceUid = seriesInstanceUid;
        this.sopInstanceUid = sopInstanceUid;
        this.isComplete = true;
        this.uri = new URI(String.format("%s://%s/%s/%s/%s/%s",
                Constants.MONGODB_SCHEME, authority, dbName, studyInstanceUid, seriesInstanceUid, sopInstanceUid));
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    public URI toURI() {
        return uri;
    }

    public String getAuthority() {
        return authority;
    }

    public String getDbName() {
        return dbName;
    }

    public String getStudyInstanceUid() {
        return studyInstanceUid;
    }

    public String getSeriesInstanceUid() {
        return seriesInstanceUid;
    }

    public String getSopInstanceUid() {
        return sopInstanceUid;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public String getFileName() {
        return String.join("/", studyInstanceUid, seriesInstanceUid, sopInstanceUid);
    }

    public static class InvalidMongoUriException extends Exception {
        private final String message;

        public InvalidMongoUriException() {
            message = "Not a valid MongoDB URI";
        }

        public InvalidMongoUriException(String customMessage) {
            message = customMessage;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public String getLocalizedMessage() {
            return message;
        }
    }
}
