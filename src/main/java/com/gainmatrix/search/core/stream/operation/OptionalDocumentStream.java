package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

/**
 * Stream copies all documents from mandatory stream and puts optional stream to visitor
 */
public final class OptionalDocumentStream<M> extends AbstractDocumentStream<M> {

    private final DocumentStream<M> mandatoryStream;

    private final DocumentStream<M> optionalStream;

    private boolean optionalEnabled;

    private boolean optionalMatched;

    public OptionalDocumentStream(M meta, DocumentStream<M> mandatoryStream, DocumentStream<M> optionalStream) {
        super(meta);

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
        return mandatoryStream.getId();
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
        long id = mandatoryStream.next();
        if (id == NO_DOCUMENT) {
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
        long id = mandatoryStream.seek(targetId);
        if (id == NO_DOCUMENT) {
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
        mandatoryStream.close();
        optionalStream.close();
    }

}
