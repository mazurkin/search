package com.gainmatrix.search.core.stream.collector.items;

import com.gainmatrix.search.core.stream.collector.WeightedCollectorItem;
import com.google.common.primitives.Longs;

public class WeightedIdItem implements WeightedCollectorItem, Comparable<WeightedIdItem> {

    public long id;

    public long weight;

    private WeightedIdItem() {
    }

    @Override
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    @Override
    public int compareTo(WeightedIdItem that) {
        return Longs.compare(this.weight, that.weight);
    }

    public static class Factory implements WeightedCollectorItem.Factory<WeightedIdItem> {

        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public WeightedIdItem create() {
            return new WeightedIdItem();
        }

        @Override
        public void copy(WeightedIdItem source, WeightedIdItem target) {
            target.id = source.id;
            target.weight = source.weight;
        }
    }
}
