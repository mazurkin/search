package com.gainmatrix.search.core.stream.container;

import com.gainmatrix.search.core.stream.operation.AbstractDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;

/**
 * Empty mock stream
 */
public final class EmptyDocumentStream<M> extends AbstractDocumentStream<M> {

    public EmptyDocumentStream(M meta) {
        super(meta);
    }

    @Override
    public DocumentStreamDescription open() {
        return DocumentStreamDescriptor.EMPTY;
    }

    @Override
    public long getId() {
        throw new IllegalStateException("Stream is empty");
    }

    @Override
    public void visit(DocumentStreamVisitor visitor) {
        throw new IllegalStateException("Stream is empty");
    }

    @Override
    public long next() {
        return NO_DOCUMENT;
    }

    @Override
    public long seek(long targetId) {
        return NO_DOCUMENT;
    }

    @Override
    public void close() {
        // nothing to close
    }

}
