package com.gainmatrix.search.lucene.util;

import com.gainmatrix.search.core.stream.AbstractMetaDocumentStream;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.description.DocumentStreamDescriptor;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class DocsEnumDocumentProxyStream<T> extends AbstractMetaDocumentStream<T> {

    private final IndexReader indexReader;

    private final String field;

    private final String value;

    private DocsEnum docsEnum;

    public DocsEnumDocumentProxyStream(T meta, IndexReader indexReader, String field, String value) {
        super(meta);

        this.indexReader = indexReader;
        this.field = field;
        this.value = value;
    }

    public static <T> DocumentStream<T> of(T meta, IndexReader indexReader, String field, String value) {
        return new DocsEnumDocumentProxyStream<T>(meta, indexReader, field, value);
    }

    @Override
    public DocumentStreamDescription open() {
        this.docsEnum = null;

        try {
            BytesRef term = new BytesRef(value);
            Terms terms = MultiFields.getTerms(indexReader, field);
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator(null);
                if (termsEnum.seekExact(term, true)) {
                    this.docsEnum = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                    return new DocumentStreamDescriptor(termsEnum.docFreq(), 0, indexReader.maxDoc());
                }
            }
        } catch (IOException e) {
            return DocumentStreamDescriptor.EMPTY;
        }

        return DocumentStreamDescriptor.EMPTY;
    }

    @Override
    public void close() {
        this.docsEnum = null;
    }

    @Override
    public long next() {
        if (docsEnum == null) {
            return NO_DOCUMENT;
        }

        try {
            int id = docsEnum.nextDoc();
            if (id != DocIdSetIterator.NO_MORE_DOCS) {
                return id;
            } else {
                return NO_DOCUMENT;
            }
        } catch (IOException e) {
            return NO_DOCUMENT;
        }
    }

    @Override
    public long seek(long targetId) {
        if (docsEnum == null) {
            return NO_DOCUMENT;
        }

        try {
            int id = docsEnum.advance((int) targetId);
            if (id != DocIdSetIterator.NO_MORE_DOCS) {
                return id;
            } else {
                return NO_DOCUMENT;
            }
        } catch (IOException e) {
            return NO_DOCUMENT;
        }
    }

    @Override
    public long getId() {
        return docsEnum.docID();
    }

}
