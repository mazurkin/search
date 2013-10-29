package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

/**
 * Stream copies all documents from mandatory stream and puts optional stream to visitor
 */
public final class OptionalDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    private final DocumentStream<M> mandatoryStream;

    private final DocumentStream<M> optionalStream;

    private long id;

    private boolean optionalEnabled;

    private boolean optionalMatched;

    public OptionalDocumentStream(M meta, DocumentStream<M> mandatoryStream, DocumentStream<M> optionalStream) {
        super(meta);

        this.id = NO_DOCUMENT;
        this.mandatoryStream = mandatoryStream;
        this.optionalStream = optionalStream;
    }

    @Override
    public DocumentStreamDescription open() {
        DocumentStreamDescription description = mandatoryStream.open();

        optionalStream.open();
        optionalEnabled = (optionalStream.next() != NO_DOCUMENT);

        return description;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        super.visit(visitor);

        mandatoryStream.visit(visitor);

        if (optionalMatched) {
            optionalStream.visit(visitor);
        }
    }

    @Override
    public long next() {
        if ((id = mandatoryStream.next()) == NO_DOCUMENT) {
            return NO_DOCUMENT;
        }

        optionalMatched = false;

        while (optionalEnabled) {
            long optionalId = optionalStream.getId();
            if (id < optionalId) {
                break;
            } else if (id > optionalId) {
                optionalEnabled = (optionalStream.seek(id) != NO_DOCUMENT);
            } else {
                optionalMatched = true;
                optionalEnabled = (optionalStream.next() != NO_DOCUMENT);
                break;
            }
        }

        return id;
    }

    @Override
    public long seek(long targetId) {
        if ((id = mandatoryStream.seek(targetId)) == NO_DOCUMENT) {
            return NO_DOCUMENT;
        }

        optionalMatched = false;

        while (optionalEnabled) {
            long optionalId = optionalStream.getId();
            if (id < optionalId) {
                break;
            } else if (id > optionalId) {
                optionalEnabled = (optionalStream.seek(id) != NO_DOCUMENT);
            } else {
                optionalMatched = true;
                optionalEnabled = (optionalStream.next() != NO_DOCUMENT);
                break;
            }
        }

        return id;
    }

    @Override
    public void close() {
        id = NO_DOCUMENT;

        mandatoryStream.close();
        optionalStream.close();
    }

}
