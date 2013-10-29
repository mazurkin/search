package com.gainmatrix.search.core.stream;

/**
 * Stream hierarchy visitor
 * @param <M> Meta description class
 */
public interface DocumentStreamVisitor<M> {

    /**
     * Informs visitor about stream took a part into boolean search
     * @param meta Meta of stream participated in search
     */
    void visit(M meta);

}
