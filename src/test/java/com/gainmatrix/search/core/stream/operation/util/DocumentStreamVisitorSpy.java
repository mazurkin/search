package com.gainmatrix.search.core.stream.operation.util;

import com.gainmatrix.search.core.stream.DocumentStreamVisitor;

import java.util.HashMap;
import java.util.Map;

public class DocumentStreamVisitorSpy<T> implements DocumentStreamVisitor<T> {

    private Map<T, Boolean> found;

    public DocumentStreamVisitorSpy() {
        this.found = new HashMap<T, Boolean>(10);
    }

    public void reset() {
        found.clear();
    }

    @Override
    public void notifyMeta(T meta) {
        found.put(meta, Boolean.TRUE);
    }

    public boolean contains(T metas) {
        return found.containsKey(metas);
    }

    public boolean check(T... metas) {
        if (this.found.size() != metas.length) {
            return false;
        }

        for (T meta : metas) {
            if (!this.found.containsKey(meta)) {
                return false;
            }
        }

        return true;
    }
}
