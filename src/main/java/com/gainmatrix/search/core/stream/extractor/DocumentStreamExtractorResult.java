package com.gainmatrix.search.core.stream.extractor;

import com.google.common.base.Preconditions;

import java.util.List;

public final class DocumentStreamExtractorResult<T> {

    private final List<T> items;

    private final int totalCount;

    private final boolean totalCountExact;

    public DocumentStreamExtractorResult(List<T> items, int totalCount, boolean totalCountExact) {
        Preconditions.checkNotNull(items, "Items container is null");

        this.items = items;
        this.totalCount = totalCount;
        this.totalCountExact = totalCountExact;
    }

    public List<T> getItems() {
        return items;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public boolean isTotalCountExact() {
        return totalCountExact;
    }
}
