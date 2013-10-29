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
 * Операционный поток-прокси реализующий операцию объединения (OR)
 */
public final class DisjunctionDocumentStream<M> extends AbstractMetaDocumentStream<M> {

    private final Collection<DocumentStream<M>> allStreams;

    private final CursorContainer<DocumentStream<M>> activeStreams;

    private long id;

    public DisjunctionDocumentStream(M meta, Collection<DocumentStream<M>> streams) {
        super(meta);

        Preconditions.checkNotNull(streams, "Container is null");

        this.id = NO_DOCUMENT;
        this.allStreams = streams;
        this.activeStreams = new CursorContainer<DocumentStream<M>>(streams.size());
    }

    @Override
    public DocumentStreamDescription open() {
        int count = 0;
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;

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

                count = count + streamCount;

                activeStreams.add(stream);
            }
        }

        return new DocumentStreamDescriptor(count, minId, maxId);
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
            if (id == stream.getId()) {
                stream.visit(visitor);
            }
        }
    }

    @Override
    public long next() {
        switch (activeStreams.size()) {
            case 0:
                return (id = NO_DOCUMENT);
            case 1:
                DocumentStream stream = activeStreams.getFirst();
                long streamId = stream.getId();

                if (id == streamId) {
                    if ((streamId = stream.next()) == NO_DOCUMENT) {
                        activeStreams.clear();
                        return (id = NO_DOCUMENT);
                    }
                }

                return (id = streamId);
            default:
                DocumentStream minimumStream = null;
                long minimumId = Long.MAX_VALUE;

                activeStreams.cursorReset();
                while (activeStreams.hasCursorNext()) {
                    DocumentStream nextStream = activeStreams.cursorNext();
                    long nextStreamId = nextStream.getId();

                    if (id == nextStreamId) {
                        if ((nextStreamId = nextStream.next()) == NO_DOCUMENT) {
                            activeStreams.cursorDelete();
                            continue;
                        }
                    }

                    if (nextStreamId < minimumId) {
                        minimumStream = nextStream;
                        minimumId = nextStreamId;
                    }
                }

                if (minimumStream != null) {
                    return (id = minimumStream.getId());
                } else {
                    return (id = NO_DOCUMENT);
                }
        }
    }

    @Override
    public long seek(long targetId) {
        switch (activeStreams.size()) {
            case 0:
                return (id = NO_DOCUMENT);
            case 1:
                DocumentStream stream = activeStreams.getFirst();
                long streamId = stream.getId();
                if (streamId < targetId) {
                    if ((streamId = stream.seek(targetId)) == NO_DOCUMENT) {
                        activeStreams.clear();
                        return (id = NO_DOCUMENT);
                    }
                }

                return (id = streamId);
            default:
                DocumentStream minimumStream = null;
                long minimumId = Long.MAX_VALUE;

                activeStreams.cursorReset();
                while (activeStreams.hasCursorNext()) {
                    DocumentStream nextStream = activeStreams.cursorNext();
                    long nextStreamId = nextStream.getId();

                    if (nextStreamId < targetId) {
                        if ((nextStreamId = nextStream.seek(targetId)) == NO_DOCUMENT) {
                            activeStreams.cursorDelete();
                            continue;
                        }
                    }

                    if (nextStreamId < minimumId) {
                        minimumStream = nextStream;
                        minimumId = nextStreamId;
                    }
                }

                if (minimumStream != null) {
                    return (id = minimumStream.getId());
                } else {
                    return (id = NO_DOCUMENT);
                }
        }
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
