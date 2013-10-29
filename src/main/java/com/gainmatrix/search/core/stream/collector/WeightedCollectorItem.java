package com.gainmatrix.search.core.stream.collector;

import com.gainmatrix.search.core.stream.DocumentStream;

public interface WeightedCollectorItem {

    long getId();

    public interface Loader<T extends WeightedCollectorItem> {

        void loadItem(DocumentStream stream, T item);
    }

    public interface Factory<T extends WeightedCollectorItem> {

        T create();

        void copy(T source, T target);
    }
}
