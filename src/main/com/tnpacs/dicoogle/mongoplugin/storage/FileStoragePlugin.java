package com.tnpacs.dicoogle.mongoplugin.storage;

import com.tnpacs.dicoogle.mongoplugin.MongoBasePlugin;
import com.tnpacs.dicoogle.mongoplugin.utils.Constants;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.io.DicomInputStream;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.StorageInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

public class FileStoragePlugin extends MongoBasePlugin implements StorageInterface {
    @Override
    public String getName() {
        return "file-storage-plugin";
    }

    @Override
    public String getScheme() {
        return Constants.FILE_SCHEME;
    }

    @Override
    public Iterable<StorageInputStream> at(URI uri, Object... objects) {
        File loc = new File(uri);
        ArrayList<StorageInputStream> dicomFiles = new ArrayList<>();
        getDicomFiles(loc, dicomFiles);
        return dicomFiles;
    }

    private void getDicomFiles(File loc, ArrayList<StorageInputStream> dicomFiles) {
        if (loc.isDirectory()) {
            File[] files = loc.listFiles();
            if (files == null) return;
            for (File file : files) {
                getDicomFiles(file, dicomFiles);
            }
        } else if (loc.getName().toLowerCase().endsWith(".dcm")) {
            dicomFiles.add(new StorageInputStream() {
                @Override
                public URI getURI() {
                    return loc.toURI();
                }

                @Override
                public InputStream getInputStream() throws IOException {
                    return new FileInputStream(loc);
                }

                @Override
                public long getSize() throws IOException {
                    return loc.length();
                }
            });
        }
    }

    @Override
    public URI store(DicomObject dicomObject, Object... objects) {
        return null;
    }

    @Override
    public URI store(DicomInputStream dicomInputStream, Object... objects) throws IOException {
        return null;
    }

    @Override
    public void remove(URI uri) {

    }
}
