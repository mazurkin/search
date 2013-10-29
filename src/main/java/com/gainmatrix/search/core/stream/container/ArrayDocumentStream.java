package com.gainmatrix.search.core.stream.container;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;
import com.google.common.base.Preconditions;

/**
 * Array-based container with binary search for seek() operation
 */
public final class ArrayDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    public final static int NO_LOOKAHEAD_STEPS = 0;

    public final static int DEFAULT_LOOKAHEAD_STEPS = 100;

    private final long[] items;

    private final long maxId;

    private final long minId;

    private final int lookaheadSteps;

    private int index;

    public ArrayDocumentStream(M meta, long[] items, int lookaheadSteps) {
        super(meta);

        Preconditions.checkNotNull(items, "Array is null");
        Preconditions.checkArgument(lookaheadSteps >= 0, "Scan step must be positive");

        this.items = items;
        this.minId = (items.length > 0) ? items[0] : Long.MAX_VALUE;
        this.maxId = (items.length > 0) ? items[items.length - 1] : Long.MIN_VALUE;
        this.lookaheadSteps = lookaheadSteps;
    }

    public ArrayDocumentStream(M meta, long[] items) {
        this(meta, items, DEFAULT_LOOKAHEAD_STEPS);
    }

    @Override
    public DocumentStreamDescription open() {
        this.index = -1;

        if (items.length > 0) {
            return new DocumentStreamDescriptor(items.length, minId, maxId);
        } else {
            return DocumentStreamDescriptor.EMPTY;
        }
    }

    @Override
    public long getId() {
        return items[index];
    }

    @Override
    public long next() {
        if (++index < items.length) {
            return items[index];
        } else {
            return NO_DOCUMENT;
        }
    }

    @Override
    public long seek(long targetId) {
        if ((lookaheadSteps > NO_LOOKAHEAD_STEPS) && (index + 1 + lookaheadSteps < items.length)) {
            ++index;
            long nextId = items[index];
            if (nextId >= targetId) {
                return nextId;
            } else if (nextId + lookaheadSteps < targetId) {
                int size = items.length - index;
                long step = (maxId - nextId) / size;
                if ((step > 0) && (nextId + lookaheadSteps * step < targetId)) {
                    return binarySeek(targetId);
                }
            }
        }

        long nextId;
        while (++index < items.length) {
            nextId = items[index];
            if (nextId >= targetId) {
                return nextId;
            }
        }

        return NO_DOCUMENT;
    }

    private long binarySeek(long targetId) {
        int leftIndex = ++index;
        int rightIndex = items.length - 1;

        while (leftIndex < rightIndex) {
            int medianIndex = (rightIndex + leftIndex) / 2;
            long medianItem = items[medianIndex];

            if (medianItem >= targetId) {
                rightIndex = medianIndex;
            } else {
                leftIndex = medianIndex + 1;
            }
        }

        if (leftIndex == rightIndex) {
            if (items[leftIndex] >= targetId) {
                index = leftIndex;
                return items[index];
            }
        }

        index = -1;
        return NO_DOCUMENT;
    }

    @Override
    public void close() {
        // nothing to close
    }

}
