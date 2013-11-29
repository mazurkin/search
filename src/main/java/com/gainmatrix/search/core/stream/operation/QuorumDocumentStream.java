package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;
import com.gainmatrix.search.core.util.CursorContainer;
import com.google.common.base.Preconditions;

import java.util.Collection;

/**
 * Операционный поток-прокси с операцией пересечения (AND) в котором участвуют по меньшей мере <b>quorum</b> потоков
 */
public final class QuorumDocumentStream<M> extends AbstractDocumentStream<M> {

    private final Collection<DocumentStream<M>> allStreams;

    private final CursorContainer<DocumentStream<M>> activeStreams;

    private int quorum;

    private long id;

    public QuorumDocumentStream(M meta, Collection<DocumentStream<M>> streams, int quorum) {
        super(meta);

        Preconditions.checkNotNull(streams, "Container is null");

        Preconditions.checkArgument(quorum > 1, "Ineffective usage - use DisjunctionDocumentStream");
        Preconditions.checkArgument(quorum < streams.size(), "Ineffective usage - use ConjunctionDocumentStream");

        this.id = NO_DOCUMENT;
        this.allStreams = streams;
        this.activeStreams = new CursorContainer<DocumentStream<M>>(streams.size());
        this.quorum = quorum;
    }

    @Override
    public DocumentStreamDescription open() {
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;

        int[] counts = new int[allStreams.size()];

        activeStreams.clear();
        for (DocumentStream<M> stream : allStreams) {
            DocumentStreamDescription description = stream.open();

            int streamCount = description.getCount();
            if ((streamCount > 0) && (stream.next() != NO_DOCUMENT)) {
                long streamMinId = description.getMinId();
                long streamMaxId = description.getMaxId();

                if (streamMinId < minId) {
                    minId = streamMinId;
                }
                if (streamMaxId > maxId) {
                    maxId = streamMaxId;
                }

                counts[activeStreams.size()] = streamCount;

                activeStreams.add(stream);
            }
        }

        // Sort all the stream by ascending number of documents so the first stream has minimal number of documents
        if (activeStreams.size() >= quorum) {
            activeStreams.sortWithWeights(counts);
            int count = calculateCount(counts, activeStreams.size(), quorum);
            return new DocumentStreamDescriptor(count, minId, maxId);
        } else {
            activeStreams.clear();
            return DocumentStreamDescriptor.EMPTY;
        }
    }

    private static int calculateCount(int[] sortedCounts, int size, int quorum) {
        long sum = 0;

        // inaccurate but simple and valid upper bound estimation
        for (int i = 0; i < size; i++) {
            sum += sortedCounts[i];
        }

        return (int) (sum / quorum);
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        super.visit(visitor);

        activeStreams.cursorReset();
        while (activeStreams.hasCursorNext()) {
            DocumentStream<M> stream = activeStreams.cursorNext();
            if (id == stream.getId()) {
                stream.visit(visitor);
            }
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public long next() {
        return seek((id == NO_DOCUMENT) ? 0 : id + 1);
    }

    @Override
    public long seek(long targetId) {
        if (activeStreams.isEmpty()) {
            return (id = NO_DOCUMENT);
        }

        long currentTargetId = targetId;

        int found = 0;
        long minNextId = Long.MAX_VALUE;

        activeStreams.cursorReset();
        while (activeStreams.hasCursorNext() && (activeStreams.size() >= quorum)) {
            DocumentStream stream = activeStreams.cursorNext();

            long streamId = stream.getId();
            if ((streamId < currentTargetId) || (streamId == id)) {
                if ((streamId = stream.seek(currentTargetId)) == NO_DOCUMENT) {
                    activeStreams.cursorDelete();
                    continue;
                }
            }

            if ((streamId > currentTargetId) && (streamId < minNextId)) {
                minNextId = streamId;
            }

            if (streamId == currentTargetId) {
                found++;
            }

            if (quorum - found >= activeStreams.size() - activeStreams.cursorIndex()) {
                activeStreams.cursorReset();
                currentTargetId = minNextId;
                found = 0;
                minNextId = Long.MAX_VALUE;
                continue;
            }

            if (found == quorum) {
                return (id = currentTargetId);
            }
        }

        activeStreams.clear();
        return (id = NO_DOCUMENT);
    }

    @Override
    public void close() {
        id = NO_DOCUMENT;

        activeStreams.clear();

        for (DocumentStream stream : allStreams) {
            stream.close();
        }
    }

}
