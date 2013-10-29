package com.gainmatrix.search.core.stream.collector.items;

import com.gainmatrix.search.core.stream.collector.WeightedCollectorItem;
import com.google.common.primitives.Longs;

public class WeightedDataItem<T> implements WeightedCollectorItem, Comparable<WeightedDataItem> {

    public long id;

    public long weight;

    public T data;

    private WeightedDataItem() {
    }

    @Override
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public int compareTo(WeightedDataItem that) {
        return Longs.compare(this.weight, that.weight);
    }

    public static class Factory implements WeightedCollectorItem.Factory<WeightedDataItem>  {

        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public WeightedDataItem create() {
            return new WeightedDataItem();
        }

        @Override
        public void copy(WeightedDataItem source, WeightedDataItem target) {
            target.id = source.id;
            target.weight = source.weight;
            target.data = source.data;
        }
    }
}
