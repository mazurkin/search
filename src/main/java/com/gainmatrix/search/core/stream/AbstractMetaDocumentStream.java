package com.gainmatrix.search.core.stream;

public abstract class AbstractMetaDocumentStream<M> implements DocumentStream<M> {

    private final M meta;

    public AbstractMetaDocumentStream(M meta) {
        this.meta = meta;
    }

    @Override
    public M getMeta() {
        return meta;
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        visitor.visit(meta);
    }

}
