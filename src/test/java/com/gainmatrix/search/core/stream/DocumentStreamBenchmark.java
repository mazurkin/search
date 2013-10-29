package com.gainmatrix.search.core.stream;

import com.gainmatrix.search.core.stream.container.ArrayDocumentStream;
import com.gainmatrix.search.core.stream.operation.ConjunctionDocumentStream;
import com.gainmatrix.search.core.stream.operation.DisjunctionDocumentStream;
import com.gainmatrix.search.core.stream.operation.NegationDocumentStream;
import com.gainmatrix.search.test.caliper.CaliperArguments;
import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

public class DocumentStreamBenchmark extends SimpleBenchmark {

    @Param
    private int size;

    @Param
    private int step;

    @Param
    private int foreahead;

    private StreamData streamData;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        streamData = new StreamData(size, step, foreahead);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        streamData.close();
    }

    public int timeSimpleStreams(int reps) {
        int count = 0;
        DocumentStream stream = streamData.stream;

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
        if ((arguments.length > 0) && "benchmark".equalsIgnoreCase(arguments[0])) {
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
        caliperArguments.addJavaArgument("gc-details-0", "-Xloggc:target/gc-streams.log");
        caliperArguments.addJavaArgument("gc-details-1", "-XX:+PrintGC");
        caliperArguments.addJavaArgument("gc-details-2", "-XX:+PrintGCDetails");
        caliperArguments.addJavaArgument("gc-details-3", "-XX:+PrintGCTimeStamps");
        caliperArguments.addJavaArgument("gc-details-4", "-XX:+PrintTenuringDistribution");
        caliperArguments.addParameters("size", Arrays.asList("100", "10000", "1000000"));
        caliperArguments.addParameters("step", Arrays.asList("20"));
        caliperArguments.addParameters("foreahead", Arrays.asList("100"));
        caliperArguments.setClazz(DocumentStreamBenchmark.class);

        Runner.main(caliperArguments.toArgumentArray());
    }

    private static void runCounter() throws Exception {
        DocumentStreamBenchmark benchmark = new DocumentStreamBenchmark();
        benchmark.setSize(1000000);
        benchmark.setStep(2);
        benchmark.setUp();
        int count = benchmark.timeSimpleStreams(1);
        benchmark.tearDown();

        System.out.printf("Streams count: " + count);
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setStep(int step) {
        this.step = step;
    }

    private static class StreamData implements Closeable {

        private long[] items1;

        private long[] items2;

        private long[] items3;

        private long[] items4;

        private DocumentStream<Void> stream;

        public StreamData(int size, int step, int foreahead) {
            items1 = new long[size];
            items2 = new long[size];
            items3 = new long[size];
            items4 = new long[size];

            Random random = new Random(0);

            int count1 = 0;
            int count2 = 0;
            int count3 = 0;
            int count4 = 0;

            for (long i = 0; (count1 < size) || (count2 < size) || (count3 < size) || (count4 < size); i++) {
                if (count1 < size && random.nextInt(step) < 1) {
                    items1[count1++] = i;
                }

                if (count2 < size && random.nextInt(step) < 1) {
                    items2[count2++] = i;
                }

                if (count3 < size && random.nextInt(step) < 1) {
                    items3[count3++] = i;
                }

                if (count4 < size && random.nextInt(step) < 1) {
                    items4[count4++] = i;
                }
            }

            DocumentStream<Void> streamSource1 = new ArrayDocumentStream<Void>(null, items1, foreahead);
            DocumentStream<Void> streamSource2 = new ArrayDocumentStream<Void>(null, items2, foreahead);
            DocumentStream<Void> streamSource3 = new ArrayDocumentStream<Void>(null, items3, foreahead);
            DocumentStream<Void> streamSource4 = new ArrayDocumentStream<Void>(null, items4, foreahead);

            // streamSource1 AND NOT streamSource2
            DocumentStream<Void> stream1 = new NegationDocumentStream<Void>(null, streamSource1, streamSource2);

            // streamSource3 OR (streamSource1 AND NOT streamSource2)
            Collection<DocumentStream<Void>> children1 = Lists.newArrayList();
            children1.add(stream1);
            children1.add(streamSource3);
            DocumentStream<Void> stream2 = new DisjunctionDocumentStream<Void>(null, children1);

            // streamSource4 AND (streamSource3 OR (streamSource1 AND NOT streamSource2))
            Collection<DocumentStream<Void>> children2 = Lists.newArrayList();
            children2.add(stream2);
            children2.add(streamSource4);
            stream = new ConjunctionDocumentStream<Void>(null, children2);
        }

        @Override
        public void close() throws IOException {
            // nothing to close
        }
    }

}
