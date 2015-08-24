package com.breedish.vinspy;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.javatuples.Pair;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.PeriodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StopWatch;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@EnableConfigurationProperties
@Configuration
public class CleanupHBaseApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(CleanupHBaseApp.class);

    public static void main(String[] args) {
        SpringApplication.run(CleanupHBaseApp.class, args);
    }

    @Value("${uri}")
    String uri;

    @Value("${ttl}")
    Integer ttl;

    @Value("${cleanTrash}")
    Boolean cleanTrash = Boolean.FALSE;

    @Override
    public void run(String... args) throws Exception {
        log.info("MongoDBUri={},TTL={},cleanTrash={}", uri, ttl, cleanTrash);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        MongoClient mongoClient = new MongoClient(uri);
        final DBCollection jobCollection = mongoClient.getDB("vinspy").getCollection("job");

        final int queueSize = 1000;
        final ArrayBlockingQueue<Pair<DBObject, TableName>> jobQueue = new ArrayBlockingQueue<>(1000);

        final org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        final HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

        ExecutorService executorService = Executors.newFixedThreadPool(300);

        DBCursor cursor = jobCollection.find(
            QueryBuilder.start("jobStatus.state").in(Lists.newArrayList("FINISHED", "ERROR"))
                .and("cleaned").is(false)
                .and("finishDate").lessThan(DateTime.now().minusHours(ttl).toDate())
                .get()
        );
        cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);

        final int total = cursor.count() + (cleanTrash ? hBaseAdmin.listTableNames().length : 0);

        final AtomicInteger progress = new AtomicInteger(0);
        final CountDownLatch counter = new CountDownLatch(total);

        log.info("Total to process: {}", total);

        for (int i = 0; i < queueSize; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Pair<DBObject, TableName> take = jobQueue.take();
                            progress.incrementAndGet();
                            if (progress.get() % 10 == 0) {
                                log.info("{}/{} Processed", progress.get(), total);
                            }
                            if (take.getValue0() != null) {
                                DBObject job = take.getValue0();
                                DBObject status = (DBObject) job.get("jobStatus");
                                final String crawlId = (String) status.get("crawlId");
                                Object id = job.get("_id");
                                if (crawlId == null) {
                                    log.error("CrawlId is null for {}", id);
                                    jobCollection.update(
                                        BasicDBObjectBuilder.start("_id", id).get(),
                                        BasicDBObjectBuilder.start("$set", new BasicDBObject("cleaned", Boolean.TRUE)).get()
                                    );
                                }

                                final String tableName = crawlId + "_webpage";

                                deleteTable(tableName, hBaseAdmin);

                                jobCollection.update(
                                    BasicDBObjectBuilder.start("_id", id).get(),
                                    BasicDBObjectBuilder.start("$set", new BasicDBObject("cleaned", Boolean.TRUE)).get()
                                );
                            } else if (take.getValue1() != null) {
                                TableName tableName = take.getValue1();
                                String timeStamp = tableName.getNameAsString().split("-")[0];
                                try {
                                    Duration duration = new Duration(new DateTime(Long.parseLong(timeStamp)), DateTime.now());
                                    if (duration.toPeriod(PeriodType.hours()).getHours() > ttl) {
                                        deleteTable(tableName.getNameAsString(), hBaseAdmin);
                                    }
                                } catch (IllegalArgumentException e) {
                                    log.error("Issue during trash cleanup", e);
                                    deleteTable(tableName.getNameAsString(), hBaseAdmin);
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            log.error("Error processing job", e);
                        } finally {
                            counter.countDown();
                        }
                    }
                }
            });
        }

        while (cursor.hasNext()) {
            jobQueue.put(new Pair<>(cursor.next(), (TableName) null));
        }

        if (cleanTrash) {
            for (TableName tableName : hBaseAdmin.listTableNames()) {
                jobQueue.put(new Pair<>((DBObject) null, tableName));
            }
        }

        stopWatch.stop();

        counter.await(60, TimeUnit.MINUTES);
        cursor.close();
        log.info("Finished in {} seconds", stopWatch.getTotalTimeSeconds());
    }

    private void deleteTable(String tableName, HBaseAdmin hBaseAdmin) throws Exception {
        if (hBaseAdmin.tableExists(tableName)) {
            if (hBaseAdmin.isTableEnabled(tableName)) {
                hBaseAdmin.disableTable(tableName);
            }
            hBaseAdmin.deleteTable(tableName);
        }
    }


}
