package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

public abstract class AbstractDocumentStream<M> implements DocumentStream<M> {

    private final M meta;

    public AbstractDocumentStream(M meta) {
        this.meta = meta;
    }

    protected M getMeta() {
        return meta;
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        visitor.notifyMeta(meta);
    }

}
