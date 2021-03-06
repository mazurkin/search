package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.DocumentStreamVisitor;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;
import com.gainmatrix.search.core.util.CursorContainer;
import com.google.common.base.Preconditions;

import java.util.Collection;

/**
 * Операционный поток-прокси реализующий операцию пересечения (AND)
 */
public final class ConjunctionDocumentStream<M> extends AbstractDocumentStream<M> {

    private final Collection<DocumentStream<M>> allStreams;

    private final CursorContainer<DocumentStream<M>> activeStreams;

    private long id;

    public ConjunctionDocumentStream(M meta, Collection<DocumentStream<M>> streams) {
        super(meta);

        Preconditions.checkNotNull(streams, "Container is null");

        this.id = NO_DOCUMENT;
        this.allStreams = streams;
        this.activeStreams = new CursorContainer<DocumentStream<M>>(streams.size());
    }

    @Override
    public DocumentStreamDescription open() {
        long minimumMaxId = Long.MAX_VALUE;
        long maximumMinId = Long.MIN_VALUE;

        int[] counts = new int[allStreams.size()];

        activeStreams.clear();
        for (DocumentStream<M> stream : allStreams) {
            DocumentStreamDescription description = stream.open();

            int streamCount = description.getCount();
            if ((streamCount > 0) && (stream.next() != NO_DOCUMENT)) {
                long streamMinId = description.getMinId();
                long streamMaxId = description.getMaxId();

                if (streamMinId > maximumMinId) {
                    maximumMinId = streamMinId;
                }
                if (streamMaxId < minimumMaxId) {
                    minimumMaxId = streamMaxId;
                }

                if (minimumMaxId < maximumMinId) {
                    activeStreams.clear();
                    break;
                }

                counts[activeStreams.size()] = streamCount;

                activeStreams.add(stream);
            } else {
                activeStreams.clear();
                break;
            }
        }

        // Sort all the stream by ascending number of documents so the first stream has minimal number of documents
        if (!activeStreams.isEmpty()) {
            activeStreams.sortWithWeights(counts);
            int count = counts[0];
            return new DocumentStreamDescriptor(count, maximumMinId, minimumMaxId);
        } else {
            return DocumentStreamDescriptor.EMPTY;
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void visit(DocumentStreamVisitor<M> visitor) {
        super.visit(visitor);

        activeStreams.cursorReset();
        while (activeStreams.hasCursorNext()) {
            DocumentStream<M> stream = activeStreams.cursorNext();
            stream.visit(visitor);
        }
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

        activeStreams.cursorReset();
        while (activeStreams.hasCursorNext()) {
            DocumentStream stream = activeStreams.cursorNext();

            long streamId = stream.getId();
            if ((streamId < currentTargetId) || (streamId == id)) {
                if ((streamId = stream.seek(currentTargetId)) == NO_DOCUMENT) {
                    activeStreams.clear();
                    return (id = NO_DOCUMENT);
                }
            }

            if (streamId > currentTargetId) {
                currentTargetId = streamId;
                if (activeStreams.cursorIndex() > 0) {
                    activeStreams.cursorReset();
                }
            }
        }

        return (id = currentTargetId);
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
