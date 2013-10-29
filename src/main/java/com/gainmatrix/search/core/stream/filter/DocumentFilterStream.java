package com.gainmatrix.search.core.stream.filter;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

/**
 * Proxy filter stream to filter documents
 */
public final class DocumentFilterStream<M> extends AbstractMetaDocumentStream<M> {

    private final DocumentStream<M> stream;

    private final DocumentFilter filter;

    public DocumentFilterStream(M meta, DocumentStream<M> stream, DocumentFilter filter) {
        super(meta);

        this.stream = stream;
        this.filter = filter;
    }

    @Override
    public DocumentStreamDescription open() {
        return stream.open();
    }

    @Override
    public long getId() {
        return stream.getId();
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        super.visit(visitor);
        stream.visit(visitor);
    }

    @Override
    public long next() {
        long id;
        while ((id = stream.next()) != NO_DOCUMENT) {
            if (filter.accept(stream)) {
                return id;
            }
        }

        return NO_DOCUMENT;
    }

    @Override
    public long seek(long targetId) {
        long id;
        if ((id = stream.seek(targetId)) != NO_DOCUMENT) {
            if (filter.accept(stream)) {
                return id;
            } else {
                return next();
            }
        }

        return NO_DOCUMENT;
    }

    @Override
    public void close() {
        stream.close();
    }
}
