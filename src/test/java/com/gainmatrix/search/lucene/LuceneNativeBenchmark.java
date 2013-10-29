package com.gainmatrix.search.lucene;

import com.gainmatrix.search.test.caliper.CaliperArguments;
import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class LuceneNativeBenchmark extends SimpleBenchmark {

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

    public int timeLucene(int reps) throws Exception {
        NullCollector collector = new NullCollector();

        for (int i = 0; i < reps; i++) {
            luceneData.searcher.search(luceneData.query, collector);
        }

        return collector.getCount();
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
        caliperArguments.setClazz(LuceneNativeBenchmark.class);

        Runner.main(caliperArguments.toArgumentArray());
    }

    private static void runCounter() throws Exception {
        LuceneNativeBenchmark benchmark = new LuceneNativeBenchmark();
        benchmark.setSize(1000000);
        benchmark.setStep(2);
        benchmark.setUp();
        int count = benchmark.timeLucene(1);
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

        private IndexSearcher searcher;

        private IndexReader reader;

        private Query query;

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

            QueryParser queryParser = new QueryParser(Version.LUCENE_44, "title", analyzer);
            query = queryParser.parse("((f1:y AND NOT f2:y) OR f3:y) AND f4:y");

            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
        }

        public void close() throws IOException {
            reader.close();
            directory.close();
        }
    }

    private static class NullCollector extends Collector {

        private int count;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            count++;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public int getCount() {
            return count;
        }
    }

}
