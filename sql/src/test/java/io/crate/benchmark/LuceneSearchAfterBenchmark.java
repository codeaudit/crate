/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.benchmark;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;
import java.util.Random;

@BenchmarkHistoryChart(filePrefix="benchmark-lucenedoccollector-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-lucenedoccollector")
public class LuceneSearchAfterBenchmark extends BenchmarkBase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private final static ESLogger logger = Loggers.getLogger(LuceneSearchAfterBenchmark.class);

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int BENCHMARK_ROUNDS = 5;
    public static final int WARMUP_ROUNDS = 1;
    public static final int NUMBER_OF_DOCS = 1_000_000;
    public static final int PAGE_SIZE = 2_000_000;

    public static final int SEARCH_AFTER = 1;

    private static int count = 0;

    private ScoreDoc lastDoc;

    private static Random random = new Random(System.nanoTime());

    private static Directory index; //new RAMDirectory();

    private final Query query = new MatchAllDocsQuery();

    private IndexSearcher searcher;

    private Sort sort;

    private static File dir;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("Preparing Data...");
        dir = new File("/tmp/lucene");
        index  = FSDirectory.open(dir);
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, analyzer);

        IndexWriter w = new IndexWriter(index, config);
        for (int i = 0; i < NUMBER_OF_DOCS; i++) {
            addDoc(w);
            if (i % 1_000_000 == 0) {
                w.commit();
            }
        }
        w.close();
        logger.info("Data prepared");


    }

    @Override
    @Before
    public void setUp() throws Exception {
        IndexReader reader = DirectoryReader.open(index);
        searcher = new IndexSearcher(reader);

        SortField sortField = new SortField("value", SortField.Type.INT);
        sort = new Sort(sortField);

        // do presearch, so we have a lastDoc
        TopFieldDocs topFieldDocs = searcher.search(query, NUMBER_OF_DOCS - SEARCH_AFTER, sort);
        lastDoc = topFieldDocs.scoreDocs[topFieldDocs.scoreDocs.length - 1];
    }

    private static void addDoc(IndexWriter w) throws IOException {
        Document doc = new Document();
        doc.add(new IntField("value", NUMBER_OF_DOCS - count, Field.Store.YES));
        count += 1;
        w.addDocument(doc);
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLucenePagingSearchAfter() throws Exception{
        int collected = 0;
        TopFieldDocs topFieldDocs = searcher.search(query, PAGE_SIZE, sort);
        ScoreDoc lastDoc = topFieldDocs.scoreDocs[topFieldDocs.scoreDocs.length - 1];
        collected += topFieldDocs.scoreDocs.length;
        while (collected < NUMBER_OF_DOCS) {
            topFieldDocs = (TopFieldDocs)searcher.searchAfter(lastDoc, query, PAGE_SIZE, sort);
            lastDoc = topFieldDocs.scoreDocs[topFieldDocs.scoreDocs.length - 1];
            logger.info("DOC ID "+lastDoc.doc);
            collected += topFieldDocs.scoreDocs.length;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneSearchAfter() throws IOException {
        searcher.searchAfter(lastDoc, query, SEARCH_AFTER, sort);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testCustomSearchAfter() throws IOException {
        NumericRangeQuery numericRangeQuery = NumericRangeQuery.newIntRange("value", (Integer)((FieldDoc)lastDoc).fields[0], null, false, true);
        searcher.search(numericRangeQuery, SEARCH_AFTER, sort);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testSearchNoSort() throws IOException {
        NumericRangeQuery numericRangeQuery = NumericRangeQuery.newIntRange("value", (Integer)((FieldDoc)lastDoc).fields[0], null, false, true);
        TopDocs docs = searcher.search(numericRangeQuery, 10);
        logger.info("DOCS "+docs.scoreDocs.length);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testMatchAllNoSort() throws IOException {
        searcher.search(new MatchAllDocsQuery(), 1);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneCustomSearchAfter() throws Exception {
        int collected = 0;
        TopFieldDocs topFieldDocs = searcher.search(query, PAGE_SIZE, sort);
        ScoreDoc lastDoc = topFieldDocs.scoreDocs[topFieldDocs.scoreDocs.length - 1];
        collected += topFieldDocs.scoreDocs.length;
        Integer lastValue = (Integer)((FieldDoc)lastDoc).fields[0];
        while (collected < NUMBER_OF_DOCS) {
            NumericRangeQuery numericRangeQuery = NumericRangeQuery.newIntRange("value", lastValue, null, true, true);
            topFieldDocs = (TopFieldDocs)searcher.searchAfter(lastDoc, numericRangeQuery, PAGE_SIZE, sort);
            collected += topFieldDocs.scoreDocs.length;
            if (collected >= NUMBER_OF_DOCS) {
                break;
            }
            lastDoc = topFieldDocs.scoreDocs[topFieldDocs.scoreDocs.length - 1];
            lastValue = (Integer)((FieldDoc)lastDoc).fields[0];
        }
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        deleteDirectory(dir);
        try {
            cluster.client().admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (IndexMissingException e) {
            // fine
        }
        cluster.afterTest();
    }

    static public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }

}