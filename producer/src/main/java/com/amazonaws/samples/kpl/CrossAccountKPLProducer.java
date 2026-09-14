// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.samples.kpl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.kinesis.producer.KinesisProducer;
import software.amazon.kinesis.producer.KinesisProducerConfiguration;
import software.amazon.kinesis.producer.UserRecordResult;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KPL 1.x producer that writes stock trade records to a Kinesis stream in Account B
 * by assuming a cross-account IAM role.
 *
 * KPL does not support specifying a stream ARN for cross-account writes (it resolves
 * stream names locally), so it must assume a role in the stream-owning account.
 * The consumer side uses resource-based policies instead (see CrossAccountKCLConsumer).
 *
 * Usage:
 *   KINESIS_EXTERNAL_ID=<non-guessable-value> \
 *   java -jar producer.jar <streamArn> <region> <crossAccountRoleArn>
 *
 * The external ID must match the one configured on the cross-account role's trust policy
 * (ExternalId parameter of stream-account-stack.yaml). It is read from the environment so it
 * is never hardcoded in source; use a unique, non-guessable value per deployment.
 */
public class CrossAccountKPLProducer {

    private static final Logger log = LoggerFactory.getLogger(CrossAccountKPLProducer.class);

    private static final int MAX_OUTSTANDING = 10_000;
    private static final long SLEEP_MS = 100;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CrossAccountKPLProducer <streamArn> <region> <crossAccountRoleArn>");
            System.exit(1);
        }
        String streamArn = args[0];
        String region = args[1];
        String crossAccountRole = args[2];
        String streamName = streamArn.substring(streamArn.lastIndexOf('/') + 1);

        // External ID is a non-guessable secret that mitigates the confused-deputy problem.
        // It is supplied via the environment so it is never hardcoded in source control.
        String externalId = System.getenv("KINESIS_EXTERNAL_ID");
        if (externalId == null || externalId.isBlank()) {
            System.err.println("Missing required environment variable KINESIS_EXTERNAL_ID "
                    + "(must match the ExternalId configured on the cross-account role's trust policy).");
            System.exit(1);
        }

        Region awsRegion = Region.of(region);
        StsClient stsClient = StsClient.builder()
                .region(awsRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        StsAssumeRoleCredentialsProvider crossAccountCredentials = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(AssumeRoleRequest.builder()
                        .roleArn(crossAccountRole)
                        .roleSessionName("kpl-cross-account-session")
                        .externalId(externalId)
                        .build())
                .build();

        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setCredentialsProvider(crossAccountCredentials)
                .setRegion(region)
                .setAggregationEnabled(true)
                .setAggregationMaxCount(100)
                .setAggregationMaxSize(51200)
                .setRecordMaxBufferedTime(100)
                .setCollectionMaxCount(500)
                .setCollectionMaxSize(5242880)
                .setRateLimit(150)
                .setMaxConnections(24)
                .setRequestTimeout(6000)
                .setConnectTimeout(6000)
                .setRecordTtl(30000)
                // Metrics are published to Account A using local credentials
                .setMetricsCredentialsProvider(DefaultCredentialsProvider.create())
                .setMetricsLevel("summary")
                .setMetricsGranularity("global")
                .setMetricsNamespace("KPLProducer");

        KinesisProducer producer = new KinesisProducer(config);
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
        AtomicLong successCount = new AtomicLong();
        AtomicLong failureCount = new AtomicLong();

        log.info("Starting producer. Stream ARN: {}, CrossAccountRole: {}", streamArn, crossAccountRole);

        try {
            while (true) {
                while (producer.getOutstandingRecordsCount() > MAX_OUTSTANDING) {
                    Thread.sleep(SLEEP_MS);
                }

                StockTrade trade = StockTrade.random();
                ByteBuffer data = ByteBuffer.wrap(trade.toJson().getBytes("UTF-8"));

                ListenableFuture<UserRecordResult> future =
                        producer.addUserRecord(streamName, trade.getTicker(), data);

                Futures.addCallback(future, new FutureCallback<UserRecordResult>() {
                    @Override
                    public void onSuccess(UserRecordResult result) {
                        successCount.incrementAndGet();
                        if (successCount.get() % 1000 == 0) {
                            log.info("Put {} records (shard: {})", successCount.get(), result.getShardId());
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        failureCount.incrementAndGet();
                        log.error("Failed to put record: {}", t.getMessage());
                    }
                }, callbackExecutor);

                Thread.sleep(10);
            }
        } finally {
            producer.flushSync();
            producer.destroy();
            callbackExecutor.shutdown();
            log.info("Done. Success: {}, Failures: {}", successCount.get(), failureCount.get());
        }
    }
}
