package com.gainmatrix.search.lucene.util;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.container.ArrayDocumentStream;
import com.gainmatrix.search.core.stream.container.EmptyDocumentStream;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public final class DocsEnumDocumentCopyStream {

    private DocsEnumDocumentCopyStream() {
    }

    public static <T> DocumentStream<T> of(T meta, IndexReader indexReader, String field, String value) {
        try {
            BytesRef term = new BytesRef(value);
            Terms terms = MultiFields.getTerms(indexReader, field);
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator(null);
                if (termsEnum.seekExact(term, true)) {
                    int size = termsEnum.docFreq();

                    long[] ids = new long[size];

                    DocsEnum docsEnum = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
                    for (int i = 0; i < size; i++) {
                        ids[i] = docsEnum.nextDoc();
                    }

                    return new ArrayDocumentStream<T>(meta, ids);
                }
            }
        } catch (IOException e) {
            return new EmptyDocumentStream<T>(meta);
        }

        return new EmptyDocumentStream<T>(meta);
    }

}
