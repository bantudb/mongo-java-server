package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryBackend extends AbstractMongoBackend {

    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName) throws MongoServerException {
        return new MemoryDatabase(this, databaseName);
    }

    @Override public void convertToCapped(String databaseName, String collectionNamed, Integer maxDocuments,
            Integer byteSize) throws MongoServerException {
        //TODO(gburd): implement...
    }

    @Override
    public void close() {
        // no-op
    }

}
