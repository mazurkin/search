package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.container.ArrayDocumentStream;
import com.gainmatrix.search.core.stream.operation.util.DocumentStreamVisitorSpy;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class DisjunctionDocumentStreamTest {

    private DocumentStream<Long> stream;

    private DocumentStreamDescription description;

    @Before
    public void setUp() throws Exception {
        long[] items1 = { 1, 2, 3, 4, 7, 8 };
        long[] items2 = { 1, 5, 7 };
        long[] items3 = { 1, 10 };

        Collection<DocumentStream<Long>> children = Lists.newArrayList();
        children.add(new ArrayDocumentStream<Long>(1001L, items1));
        children.add(new ArrayDocumentStream<Long>(1002L, items2));
        children.add(new ArrayDocumentStream<Long>(1003L, items3));

        stream = new DisjunctionDocumentStream<Long>(1000L, children);
        description = stream.open();
    }

    @After
    public void tearDown() throws Exception {
        stream.close();
    }

    @Test
    public void testDescription() throws Exception {
        Assert.assertEquals(11, description.getCount());
        Assert.assertEquals(1, description.getMinId());
        Assert.assertEquals(10, description.getMaxId());
    }

    @Test
    public void testNext() throws Exception {
        Assert.assertEquals(1, stream.next());
        Assert.assertEquals(1, stream.getId());

        Assert.assertEquals(2, stream.next());
        Assert.assertEquals(2, stream.getId());

        Assert.assertEquals(3, stream.next());
        Assert.assertEquals(3, stream.getId());

        Assert.assertEquals(4, stream.next());
        Assert.assertEquals(4, stream.getId());

        Assert.assertEquals(5, stream.next());
        Assert.assertEquals(5, stream.getId());

        Assert.assertEquals(7, stream.next());
        Assert.assertEquals(7, stream.getId());

        Assert.assertEquals(8, stream.next());
        Assert.assertEquals(8, stream.getId());

        Assert.assertEquals(10, stream.next());
        Assert.assertEquals(10, stream.getId());

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.next());
    }

    @Test
    public void testSeek() throws Exception {
        Assert.assertEquals(3L, stream.seek(3L));
        Assert.assertEquals(3L, stream.getId());

        Assert.assertEquals(8L, stream.seek(8L));
        Assert.assertEquals(8L, stream.getId());

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.seek(12L));

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.next());
    }

    @Test
    public void testSeekSame() throws Exception {
        Assert.assertEquals(3L, stream.seek(3L));
        Assert.assertEquals(3L, stream.getId());

        Assert.assertEquals(4L, stream.seek(3L));
        Assert.assertEquals(4L, stream.getId());
    }

    @Test
    public void testVisit() throws Exception {
        DocumentStreamVisitorSpy<Long> spy = new DocumentStreamVisitorSpy<Long>();

        spy.reset();
        Assert.assertEquals(4L, stream.seek(4L));
        stream.visit(spy);
        Assert.assertTrue(spy.check(1000L, 1001L));

        spy.reset();
        Assert.assertEquals(5L, stream.next());
        stream.visit(spy);
        Assert.assertTrue(spy.check(1000L, 1002L));

        spy.reset();
        Assert.assertEquals(7L, stream.next());
        stream.visit(spy);
        Assert.assertTrue(spy.check(1000L, 1001L, 1002L));

        spy.reset();
        Assert.assertEquals(10L, stream.seek(10L));
        stream.visit(spy);
        Assert.assertTrue(spy.check(1000L, 1003L));
    }

    @Test
    public void testEmpty() throws Exception {
        Collection<DocumentStream<Void>> childStreams = new ArrayList<DocumentStream<Void>>();
        childStreams.add(new ArrayDocumentStream<Void>(null, ArrayUtils.EMPTY_LONG_ARRAY));
        childStreams.add(new ArrayDocumentStream<Void>(null, ArrayUtils.EMPTY_LONG_ARRAY));

        DocumentStream stream = new DisjunctionDocumentStream<Void>(null, childStreams);
        stream.open();

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.next());

        stream.close();
    }
}
