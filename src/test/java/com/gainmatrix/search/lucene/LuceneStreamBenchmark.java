package com.gainmatrix.search.lucene;

import com.gainmatrix.search.core.stream.DocumentStream;
import com.gainmatrix.search.core.stream.operation.ConjunctionDocumentStream;
import com.gainmatrix.search.core.stream.operation.DisjunctionDocumentStream;
import com.gainmatrix.search.core.stream.operation.NegationDocumentStream;
import com.gainmatrix.search.lucene.util.DocsEnumDocumentProxyStream;
import com.gainmatrix.search.test.caliper.CaliperArguments;
import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

public class LuceneStreamBenchmark extends SimpleBenchmark {

    @Param
    private int size;

    @Param
    private int step;

    private LuceneData luceneData;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        luceneData = new LuceneData(size, step);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        luceneData.close();
    }

    public int timeLuceneStreams(int reps) {
        int count = 0;
        DocumentStream stream = luceneData.stream;

        for (int i = 0; i < reps; i++) {
            stream.open();
            while (stream.next() != DocumentStream.NO_DOCUMENT) {
                count++;
            }
            stream.close();
        }

        return count;
    }

    public static void main(String[] arguments) throws Exception {
        if ((arguments.length > 0) && "benchmark".equals(arguments[0])) {
            runCaliper();
        } else {
            runCounter();
        }
    }

    private static void runCaliper() throws Exception {
        CaliperArguments caliperArguments = new CaliperArguments();
        caliperArguments.setRunMs(10000);
        caliperArguments.setTrials(1);
        caliperArguments.addJavaArgument("memoryMin", "-Xms512m");
        caliperArguments.addJavaArgument("memoryMax", "-Xmx512m");
        caliperArguments.addJavaArgument("gc-mode-0", "-XX:+UseConcMarkSweepGC");
        caliperArguments.addJavaArgument("gc-details-0", "-Xloggc:target/gc-lucene.log");
        caliperArguments.addJavaArgument("gc-details-1", "-XX:+PrintGC");
        caliperArguments.addJavaArgument("gc-details-2", "-XX:+PrintGCDetails");
        caliperArguments.addJavaArgument("gc-details-3", "-XX:+PrintGCTimeStamps");
        caliperArguments.addJavaArgument("gc-details-4", "-XX:+PrintTenuringDistribution");
        caliperArguments.addParameters("size", Arrays.asList("100", "10000", "1000000"));
        caliperArguments.addParameters("step", Arrays.asList("20"));
        caliperArguments.setClazz(LuceneStreamBenchmark.class);

        Runner.main(caliperArguments.toArgumentArray());
    }

    private static void runCounter() throws Exception {
        LuceneStreamBenchmark benchmark = new LuceneStreamBenchmark();
        benchmark.setSize(1000000);
        benchmark.setStep(2);
        benchmark.setUp();
        int count = benchmark.timeLuceneStreams(1);
        benchmark.tearDown();

        System.out.println("Lucene count: " + count);
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setStep(int step) {
        this.step = step;
    }

    private static class LuceneData implements Closeable {

        private RAMDirectory directory;

        private IndexReader reader;

        private DocumentStream<Void> stream;

        public LuceneData(int size, int step) throws Exception {
            directory = new RAMDirectory();

            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);

            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, analyzer);

            IndexWriter writer = new IndexWriter(directory, indexWriterConfig);

            Random random = new Random(0);

            int f1count = 0;
            int f2count = 0;
            int f3count = 0;
            int f4count = 0;

            while ((f1count < size) || (f2count < size) || (f3count < size) || (f4count < size)) {
                Document document = new Document();
                if (f1count < size && random.nextInt(step) < 1) {
                    document.add(new StringField("f1", "y", Field.Store.NO));
                    f1count++;
                }
                if (f2count < size && random.nextInt(step) < 1) {
                    document.add(new StringField("f2", "y", Field.Store.NO));
                    f2count++;
                }
                if (f3count < size && random.nextInt(step) < 1) {
                    document.add(new StringField("f3", "y", Field.Store.NO));
                    f3count++;
                }
                if (f4count < size && random.nextInt(step) < 1) {
                    document.add(new StringField("f4", "y", Field.Store.NO));
                    f4count++;
                }
                writer.addDocument(document);
            }

            writer.forceMerge(1);
            writer.close();

            reader = DirectoryReader.open(directory);

            DocumentStream<Void> streamSource1 = DocsEnumDocumentProxyStream.of(null, reader, "f1", "y");
            DocumentStream<Void> streamSource2 = DocsEnumDocumentProxyStream.of(null, reader, "f2", "y");
            DocumentStream<Void> streamSource3 = DocsEnumDocumentProxyStream.of(null, reader, "f3", "y");
            DocumentStream<Void> streamSource4 = DocsEnumDocumentProxyStream.of(null, reader, "f4", "y");

            // streamSource1 AND NOT streamSource2
            DocumentStream<Void> stream1 = new NegationDocumentStream<Void>(null, streamSource1, streamSource2);

            // streamSource3 OR (streamSource1 AND NOT streamSource2)
            Collection<DocumentStream<Void>> children1 = Lists.newArrayList();
            children1.add(stream1);
            children1.add(streamSource3);
            DocumentStream <Void> stream2 = new DisjunctionDocumentStream<Void>(null, children1);

            // streamSource4 AND (streamSource3 OR (streamSource1 AND NOT streamSource2))
            Collection<DocumentStream<Void>> children2 = Lists.newArrayList();
            children2.add(stream2);
            children2.add(streamSource4);
            stream = new ConjunctionDocumentStream<Void>(null, children2);
            stream.open();
        }

        public void close() throws IOException {
            stream.close();
            reader.close();
            directory.close();
        }
    }

}
