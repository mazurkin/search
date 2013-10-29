package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
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
public final class ConjunctionDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    private final Collection<DocumentStream<M>> allStreams;

    private final CursorContainer<DocumentStream<M>> activeStreams;

    private final int[] counts;

    private long id;

    public ConjunctionDocumentStream(M meta, Collection<DocumentStream<M>> streams) {
        super(meta);

        Preconditions.checkNotNull(streams, "Container is null");

        this.id = NO_DOCUMENT;
        this.allStreams = streams;
        this.activeStreams = new CursorContainer<DocumentStream<M>>(streams.size());
        this.counts = new int[streams.size()];
    }

    @Override
    public DocumentStreamDescription open() {
        int count = Integer.MAX_VALUE;
        long minimumMaxId = Long.MAX_VALUE;
        long maximumMinId = Long.MIN_VALUE;

        activeStreams.clear();
        for (DocumentStream<M> stream : allStreams) {
            DocumentStreamDescription description = stream.open();

            int streamCount = description.getCount();
            if (streamCount > 0) {
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

                if (streamCount < count) {
                    count = streamCount;
                }

                counts[activeStreams.size()] = streamCount;

                activeStreams.add(stream);
            } else {
                activeStreams.clear();
                break;
            }
        }

        // Сортируем потоки по возрастанию потенциального количества документов в потоке - таким образом сначала идут
        // потоки с наименьшим количеством документов
        if (activeStreams.size() > 0) {
            activeStreams.sortWithWeights(counts);
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

        id = targetId;

        // Исполняем цикл поиска одинакового идентификатора во всех потоках
        int streamProgressIndex = -1;

        activeStreams.cursorReset();
        while (activeStreams.hasCursorNext()) {
            DocumentStream stream = activeStreams.cursorNext();
            int streamIndex = activeStreams.cursorIndex();

            long streamId;
            if ((streamIndex > streamProgressIndex) || ((streamId = stream.getId()) < id)) {
                if ((streamId = stream.seek(id)) == NO_DOCUMENT) {
                    activeStreams.clear();
                    return (id = NO_DOCUMENT);
                }
            }

            if (streamId > id) {
                id = streamId;
                if (activeStreams.cursorIndex() > 0) {
                    activeStreams.cursorReset();
                }
            }

            if (streamIndex > streamProgressIndex) {
                streamProgressIndex = streamIndex;
            }
        }

        return id;
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
