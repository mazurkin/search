package com.gainmatrix.search.core.stream.container;

import com.gainmatrix.search.core.stream.DocumentStream;
import junit.framework.Assert;
import org.junit.Test;

public class ArrayDocumentStreamTest {

    @Test
    public void testSeek() throws Exception {
        long[] ids = { 1, 4, 5, 6, 8, 20, 26, 100, 2000, 3473, 3489, 9003, 10837, 10839, 10841, 10856, 10987, 10999 };

        DocumentStream stream = new ArrayDocumentStream<Void>(null, ids, 3);
        stream.open();

        Assert.assertEquals(4, stream.seek(3));

        Assert.assertEquals(5, stream.seek(4));

        Assert.assertEquals(6, stream.seek(5));

        Assert.assertEquals(2000, stream.seek(1999));
        Assert.assertEquals(10856, stream.seek(10856));
        Assert.assertEquals(DocumentStream.NO_DOCUMENT, stream.seek(20000));

        stream.close();
    }

}
