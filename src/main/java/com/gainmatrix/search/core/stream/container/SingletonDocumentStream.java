package com.gainmatrix.search.core.stream.container;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;

/**
 * Stream for a singleton document
 */
public final class SingletonDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    private final long id;

    private int index;

    public SingletonDocumentStream(M meta, long id) {
        super(meta);

        this.id = id;
    }

    @Override
    public DocumentStreamDescription open() {
        this.index = -1;

        return new DocumentStreamDescriptor(1, id, id);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public long next() {
        if (++index == 0) {
            return id;
        } else {
            return NO_DOCUMENT;
        }
    }

    @Override
    public long seek(long targetId) {
        while (++index == 0) {
            if (id >= targetId) {
                return id;
            }
        }

        return NO_DOCUMENT;
    }

    @Override
    public void close() {
        // nothing to close
    }

}
