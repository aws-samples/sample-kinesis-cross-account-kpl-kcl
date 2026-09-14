// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.samples.kcl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.arns.Arn;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.UUID;

/**
 * KCL 3.x consumer that reads stock trade records from a Kinesis stream in Account B
 * using resource-based policy access.
 *
 * The stream in Account B has a resource policy granting read access to this account
 * (Account C). KCL 3.x natively supports cross-account access via stream ARN — no
 * IAM role assumption is needed. All clients use local credentials.
 *
 * This is the recommended pattern for consumers (simpler than AssumeRole). The producer
 * side still uses AssumeRole due to a KPL limitation.
 *
 * Usage:
 *   Polling:  java -jar consumer.jar <streamArn> <region> <applicationName>
 *   EFO:     java -jar consumer.jar <streamArn> <region> <applicationName> --efo <consumerArn>
 */
public class CrossAccountKCLConsumer {

    private static final Logger log = LoggerFactory.getLogger(CrossAccountKCLConsumer.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CrossAccountKCLConsumer <streamArn> <region> <applicationName> [--efo <consumerArn>]");
            System.exit(1);
        }
        String streamArn       = args[0];
        String region          = args[1];
        String applicationName = args[2];
        boolean useEfo         = args.length > 3 && "--efo".equals(args[3]);
        String consumerArn     = useEfo && args.length > 4 ? args[4] : null;

        if (useEfo && consumerArn == null) {
            System.err.println("ERROR: --efo requires a consumerArn argument.");
            System.err.println("The stream owner (Account B) must pre-register the consumer and provide its ARN.");
            System.exit(1);
        }

        Region awsRegion = Region.of(region);

        // All clients use local credentials (EC2 instance profile in Account C).
        // The stream's resource-based policy in Account B authorizes Kinesis read access.
        var kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                software.amazon.awssdk.services.kinesis.KinesisAsyncClient.builder()
                        .region(awsRegion)
                        .credentialsProvider(DefaultCredentialsProvider.create()));

        var dynamoClient = software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient.builder()
                .region(awsRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        var cloudWatchClient = software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient.builder()
                .region(awsRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        ShardRecordProcessorFactory processorFactory = StockTradeProcessor::new;

        // Use Arn constructor — required for cross-account stream access in KCL 3.x
        Arn streamArnObj = Arn.fromString(streamArn);

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamArnObj,
                applicationName,
                kinesisClient,
                dynamoClient,
                cloudWatchClient,
                UUID.randomUUID().toString(),
                processorFactory);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),

                // Default coordinator config runs as native KCL 3.x. For a new application,
                // KCL 3.5+ uses the single table format automatically: all of KCL's bookkeeping
                // (leases, worker metrics, coordinator state) lives in one DynamoDB lease table,
                // so no separate -WorkerMetricStats / -CoordinatorState tables are created.
                configsBuilder.coordinatorConfig(),

                configsBuilder.leaseManagementConfig()
                        .failoverTimeMillis(10_000)
                        .shardSyncIntervalMillis(60_000)
                        .maxLeasesForWorker(Integer.MAX_VALUE),

                configsBuilder.lifecycleConfig()
                        .taskBackoffTimeMillis(500),

                configsBuilder.metricsConfig()
                        .metricsLevel(MetricsLevel.SUMMARY),

                configsBuilder.processorConfig()
                        .callProcessRecordsEvenForEmptyRecordList(false),

                configsBuilder.retrievalConfig()
                        .retrievalSpecificConfig(useEfo
                                ? new FanOutConfig(kinesisClient)
                                        .consumerArn(consumerArn)
                                : new PollingConfig(streamArn, kinesisClient)
                                        .maxRecords(10_000)
                                        .idleTimeBetweenReadsInMillis(1_500)));

        log.info("Starting KCL consumer. Stream: {}, App: {}, Mode: {}{}",
                streamArn, applicationName, useEfo ? "EFO" : "POLLING",
                useEfo ? ", ConsumerArn: " + consumerArn : "");

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down scheduler...");
            try {
                scheduler.startGracefulShutdown().get();
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }
        }));

        schedulerThread.join();
    }
}
