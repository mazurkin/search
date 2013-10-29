package com.gainmatrix.search.core.stream.operation;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.DocumentStreamDescription;
import com.gainmatrix.search.core.stream.container.ArrayDocumentStream;
import com.gainmatrix.search.core.stream.operation.util.DocumentStreamVisitorSpy;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OptionalDocumentStreamTest {

    private DocumentStream<Long> stream;

    private DocumentStreamDescription description;

    @Before
    public void setUp() throws Exception {
        long[] items1 = { 1, 2, 3, 7, 8 };
        long[] items2 = { 1, 5, 7 };

        DocumentStream<Long> mandatory = new ArrayDocumentStream<Long>(1001L, items1);
        DocumentStream<Long> optional = new ArrayDocumentStream<Long>(1002L, items2);

        stream = new OptionalDocumentStream<Long>(1000L, mandatory, optional);
        description = stream.open();
    }

    @After
    public void tearDown() throws Exception {
        stream.close();
    }

    @Test
    public void testDescription() throws Exception {
        Assert.assertEquals(5, description.getCount());
        Assert.assertEquals(1, description.getMinId());
        Assert.assertEquals(8, description.getMaxId());
    }

    @Test
    public void testNext() throws Exception {
        Assert.assertEquals(1, stream.next());
        Assert.assertEquals(1, stream.getId());

        Assert.assertEquals(2, stream.next());
        Assert.assertEquals(2, stream.getId());

        Assert.assertEquals(3, stream.next());
        Assert.assertEquals(3, stream.getId());

        Assert.assertEquals(7, stream.next());
        Assert.assertEquals(7, stream.getId());

        Assert.assertEquals(8, stream.next());
        Assert.assertEquals(8, stream.getId());

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.next());
    }

    @Test
    public void testSeek() throws Exception {
        Assert.assertEquals(3, stream.seek(3L));
        Assert.assertEquals(3, stream.getId());

        Assert.assertEquals(7, stream.seek(5L));
        Assert.assertEquals(7, stream.getId());

        Assert.assertEquals(8, stream.seek(8L));
        Assert.assertEquals(8, stream.getId());

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.seek(9L));

        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.next());
    }

    @Test
    public void testVisit() throws Exception {
        DocumentStreamVisitorSpy<Long> spy = new DocumentStreamVisitorSpy<Long>();

        spy.reset();
        Assert.assertEquals(3L, stream.seek(3L));
        stream.visit(spy);
        Assert.assertTrue(spy.equals(1000L, 1001L));

        spy.reset();
        Assert.assertEquals(7L, stream.next());
        stream.visit(spy);
        Assert.assertTrue(spy.equals(1000L, 1001L, 1002L));
    }
}
