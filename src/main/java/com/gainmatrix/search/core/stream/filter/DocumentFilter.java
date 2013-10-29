package com.gainmatrix.search.core.stream.filter;

import com.gainmatrix.search.core.stream.DocumentStreamState;

/**
 * Document filter
 */
public interface DocumentFilter {

    /**
     * Checks current document weither to include it to output stream
     * @param streamState Current document strea,
     * @return Return 'true' to include document to output stream or 'false' to throw it away
     */
    boolean accept(DocumentStreamState streamState);

}
