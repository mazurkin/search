package com.gainmatrix.search.core.stream.collector;

import java.util.*;

public final class WeightedCollector<T extends WeightedCollectorItem & Comparable<T>> {

    private final int capacity;

    private final Queue<T> queue;

    private final WeightedCollectorItem.Factory<T> itemFactory;

    private int total;

    public WeightedCollector(WeightedCollectorItem.Factory<T> itemFactory, int capacity) {
        this.capacity = capacity;
        this.itemFactory = itemFactory;
        this.queue = new PriorityQueue<T>(capacity);
        this.total = 0;
    }

    public int total() {
        return total;
    }

    public int size() {
        return queue.size();
    }

    public void reset() {
        queue.clear();
        total = 0;
    }

    public List<T> slice(int offset, int count) {
        List<T> result;

        if (queue.size() > offset) {
            while (queue.size() > offset + count) {
                queue.poll();
            }

            int size = queue.size() - offset;

            result = new ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                result.add(queue.poll());
            }

            T temp;
            for (int left = 0, right = size - 1; left < right; left++, right--) {
                temp = result.get(left);
                result.set(left, result.get(right));
                result.set(right, temp);
            }
        } else {
            result = Collections.emptyList();
        }

        queue.clear();

        return result;
    }

    public void collect(T permanentSource) {
        total++;

        if (queue.size() < capacity) {
            T entry = itemFactory.create();
            itemFactory.copy(permanentSource, entry);
            queue.add(entry);
        } else if (permanentSource.compareTo(queue.peek()) > 0) {
            T entry = queue.poll();
            itemFactory.copy(permanentSource, entry);
            queue.add(entry);
        }
    }

}
