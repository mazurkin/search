package com.gainmatrix.search.core.stream.description;

import com.gainmatrix.search.core.stream.DocumentStreamDescription;

/**
 * Stream description implementation
 */
public final class DocumentStreamDescriptor implements DocumentStreamDescription {

    public static final DocumentStreamDescription EMPTY =
        new DocumentStreamDescriptor(0, Long.MAX_VALUE, Long.MIN_VALUE);

    private final long minId;

    private final long maxId;

    private final int count;

    public DocumentStreamDescriptor(int count, long minId, long maxId) {
        this.count = count;
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public long getMinId() {
        return minId;
    }

    @Override
    public long getMaxId() {
        return maxId;
    }

}
