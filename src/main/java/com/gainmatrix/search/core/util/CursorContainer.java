package com.gainmatrix.search.core.util;

import java.util.Collection;
import java.util.Comparator;

/**
 * Контейнер-коллекция в виде обертки над массивом предназначен для хранения элементов в массиве число которых не
 * превышает установленный при создании контейнера лимит. Обладает встроенным синглтон-итератором, что позволяет
 * снизить нагрузку на кучу при интенсивных операциях.
 * @param <T> Тип хранимого элемента
 */
public final class CursorContainer<T> {

    private Object[] items;

    private int count;

    private int index;

    public CursorContainer(int capacity) {
        this.items = new Object[capacity];
        this.count = 0;
        this.index = -1;
    }

    public void add(T item) {
        if (count < items.length) {
            this.items[count] = item;
            this.count++;
        } else {
            throw new IndexOutOfBoundsException("Container is full");
        }
    }

    public void addAll(Collection<T> items) {
        for (T item : items) {
            add(item);
        }
    }

    @SuppressWarnings("unchecked")
    public T get(int index) {
        if (index < count) {
            return (T) items[index];
        } else {
            throw new IndexOutOfBoundsException("Index exceeds size: " + index);
        }
    }

    @SuppressWarnings("unchecked")
    public T getFirst() {
        if (count > 0) {
            return (T) items[0];
        } else {
            throw new IndexOutOfBoundsException("No elements");
        }
    }

    @SuppressWarnings("unchecked")
    public T getLast() {
        if (count > 0) {
            return (T) items[count - 1];
        } else {
            throw new IndexOutOfBoundsException("No elements");
        }
    }

    public void delete(int index) {
        if (index < count) {
            if (index < count - 1) {
                items[index] = items[count - 1];
            }
            items[count - 1] = null;
            count--;
        } else {
            throw new IndexOutOfBoundsException("Index exceeds size: " + index);
        }
    }

    public int size() {
        return count;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public void clear() {
        for (int i= 0; i < count; i++) {
            items[i] = null;
        }
        this.count = 0;
    }

    public void swap(int index1, int index2) {
        if ((index1 < count) && (index2 < count)) {
            if (index1 != index2) {
                Object tmp = items[index1];
                items[index1] = items[index2];
                items[index2] = tmp;
            }
        } else {
            throw new IndexOutOfBoundsException("Indexes exceeds size: " + index1 + " or " + index2);
        }
    }

    public int minimumIndex(Comparator<T> comparator) {
        if (count == 0) {
            throw new IllegalStateException("Stream is empty");
        }

        @SuppressWarnings("unchecked")
        T minimumItem = (T) items[0];
        int minimumIndex = 0;

        for (int i = 1; i < count; i++) {
            @SuppressWarnings("unchecked")
            T item = (T) items[i];
            if (comparator.compare(item, minimumItem) < 0) {
                minimumItem = item;
                minimumIndex = i;
            }
        }

        return minimumIndex;
    }

    public int maximumIndex(Comparator<T> comparator) {
        if (count == 0) {
            throw new IllegalStateException("Stream is empty");
        }

        @SuppressWarnings("unchecked")
        T maximumItem = (T) items[0];
        int maximumIndex = 0;

        for (int i = 1; i < count; i++) {
            @SuppressWarnings("unchecked")
            T item = (T) items[i];
            if (comparator.compare(item, maximumItem) > 0) {
                maximumItem = item;
                maximumIndex = i;
            }
        }

        return maximumIndex;
    }

    public void sortWithWeights(int[] weights) {
        if ((weights == null) || (weights.length < count)) {
            throw new IllegalArgumentException("Weight array is too small");
        }

        if (count < 2) {
            return;
        }

        sortWithWeightInternal(weights, 0, count - 1);
    }

    private void sortWithWeightInternal(int[] weights, int leftLimit, int rightLimit) {
        int leftIndex = leftLimit;
        int rightIndex = rightLimit;

        int pivot = weights[(leftIndex + rightIndex) >>> 1];

        while (leftIndex <= rightIndex) {
            while (weights[leftIndex] < pivot) {
                leftIndex++;
            }

            while (pivot < weights[rightIndex]) {
                rightIndex--;
            }

            if (leftIndex <= rightIndex) {
                int tempWeight = weights[leftIndex];
                weights[leftIndex] = weights[rightIndex];
                weights[rightIndex] = tempWeight;

                Object tempItem = items[leftIndex];
                items[leftIndex] = items[rightIndex];
                items[rightIndex] = tempItem;

                leftIndex++;
                rightIndex--;
            }
        }

        if (leftLimit < rightIndex) {
            sortWithWeightInternal(weights, leftLimit, rightIndex);
        }

        if (leftIndex < rightLimit) {
            sortWithWeightInternal(weights, leftIndex, rightLimit);
        }
    }

    public void cursorReset() {
        this.index = -1;
    }

    public T cursorNext() {
        index++;
        return get(index);
    }

    public void cursorDelete() {
        delete(index);
        index--;
    }

    public int cursorIndex() {
        return index;
    }

    public boolean hasCursorNext() {
        return index < count - 1;
    }

}
