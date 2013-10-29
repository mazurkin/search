package com.gainmatrix.search.core.stream.extractor;

import com.gainmatrix.search.core.stream.collector.WeightedCollector;
import com.gainmatrix.search.core.stream.collector.WeightedCollectorItem;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;

import java.util.Collections;
import java.util.List;

public final class DocumentStreamExtractor<T extends WeightedCollectorItem & Comparable<T>> {

    private final WeightedCollectorItem.Factory<T> factory;

    private final WeightedCollectorItem.Loader<T> loader;

    protected DocumentStreamExtractor(WeightedCollectorItem.Factory<T> factory, WeightedCollectorItem.Loader<T> loader) {
        this.factory = factory;
        this.loader = loader;
    }

    public DocumentStreamExtractorResult<T> collect(DocumentStream stream, int offset, int count) {
        WeightedCollector<T> collector = new WeightedCollector<T>(factory, offset + count);

        DocumentStreamDescription description = stream.open();
        try {
            if (description.getCount() < offset) {
                return new DocumentStreamExtractorResult<T>(Collections.<T>emptyList(), description.getCount(), false);
            }

            T item = factory.create();
            while (stream.next() != DocumentStream.NO_DOCUMENT) {
                loader.loadItem(stream, item);
                collector.collect(item);
            }
        } finally {
            stream.close();
        }

        List<T> items = collector.slice(offset, collector.size() - offset);
        return new DocumentStreamExtractorResult<T>(items, collector.total(), true);
    }

}
