package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

/**
 * Операционный поток-прокси реализующий операцию вычитания (AND NOT)
 */
public final class NegationDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    private final DocumentStream<M> positiveStream;

    private final DocumentStream<M> negativeStream;

    private boolean activeFilter;

    public NegationDocumentStream(M meta, DocumentStream<M> positiveStream, DocumentStream<M> negativeStream) {
        super(meta);

        this.positiveStream = positiveStream;
        this.negativeStream = negativeStream;
    }

    @Override
    public DocumentStreamDescription open() {
        DocumentStreamDescription description = positiveStream.open();

        negativeStream.open();
        activeFilter = (negativeStream.next() != NO_DOCUMENT);

        return description;
    }

    @Override
    public long getId() {
        return positiveStream.getId();
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        super.visit(visitor);
        positiveStream.visit(visitor);
    }

    @Override
    public long next() {
        long positiveId;
        if ((positiveId = positiveStream.next()) == NO_DOCUMENT) {
            return NO_DOCUMENT;
        }

        while (activeFilter) {
            long negativeId = negativeStream.getId();
            if (positiveId < negativeId) {
                break;
            } else if (positiveId > negativeId) {
                activeFilter = (negativeStream.seek(positiveId) != NO_DOCUMENT);
            } else {
                if ((positiveId = positiveStream.next()) == NO_DOCUMENT) {
                    return NO_DOCUMENT;
                }
                activeFilter = (negativeStream.next() != NO_DOCUMENT);
            }
        }

        return positiveId;
    }

    @Override
    public long seek(long targetId) {
        long positiveId;
        if ((positiveId = positiveStream.seek(targetId)) == NO_DOCUMENT) {
            return NO_DOCUMENT;
        }

        while (activeFilter) {
            long negativeId = negativeStream.getId();
            if (positiveId < negativeId) {
                break;
            } else if (positiveId > negativeId) {
                activeFilter = (negativeStream.seek(positiveId) != NO_DOCUMENT);
            } else if (positiveId == negativeId) {
                if ((positiveId = positiveStream.next()) == NO_DOCUMENT) {
                    return NO_DOCUMENT;
                }
                activeFilter = (negativeStream.next() != NO_DOCUMENT);
            }
        }

        return positiveId;
    }

    @Override
    public void close() {
        positiveStream.close();
        negativeStream.close();
    }

}
