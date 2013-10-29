package com.gainmatrix.search.core.stream.extractor;

import com.gainmatrix.search.core.stream.collector.WeightedCollectorItem;
import com.gainmatrix.search.core.stream.collector.items.WeightedIdItem;
import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.container.ArrayDocumentStream;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DocumentStreamExtractorTest {

    @Test
    public void testCommon() throws Exception {
        long[] items = { 1, 3, 4, 5, 6, 7, 10, 20, 30, 100, 3000, 4487, 9742, 18782 };

        WeightedCollectorItem.Loader<WeightedIdItem> loader =
            new WeightedCollectorItem.Loader<WeightedIdItem>() {
                @Override
                public void loadItem(DocumentStream stream, WeightedIdItem item) {
                    item.setId(stream.getId());
                    item.setWeight(stream.getId());
                }
            };

        DocumentStreamExtractor<WeightedIdItem> collector =
            new DocumentStreamExtractor<WeightedIdItem>(WeightedIdItem.Factory.INSTANCE, loader);

        DocumentStream stream;
        DocumentStreamExtractorResult<WeightedIdItem> result;

        stream = new ArrayDocumentStream<Void>(null, items);
        result = collector.collect(stream, 0, 2);
        assertItemEquals(Arrays.asList(18782L, 9742L), result.getItems());

        stream = new ArrayDocumentStream<Void>(null, items);
        result = collector.collect(stream, 5, 4);
        assertItemEquals(Arrays.asList(30L, 20L, 10L, 7L), result.getItems());

        stream = new ArrayDocumentStream<Void>(null, items);
        result = collector.collect(stream, 12, 5);
        assertItemEquals(Arrays.asList(3L, 1L), result.getItems());

        stream = new ArrayDocumentStream<Void>(null, items);
        result = collector.collect(stream, 15, 5);
        assertItemEquals(Arrays.<Long>asList(), result.getItems());
    }

    private void assertItemEquals(List<Long> ids, List<WeightedIdItem> items) {
        Assert.assertEquals(ids.size(), items.size());
        Iterator<Long> idIterator = ids.iterator();
        Iterator<WeightedIdItem> itemIterator = items.iterator();
        while (idIterator.hasNext()) {
            Assert.assertEquals((long) idIterator.next(), itemIterator.next().getId());
        }
    }

}
