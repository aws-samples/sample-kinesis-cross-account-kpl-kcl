// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.samples.kcl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import java.nio.charset.StandardCharsets;

/**
 * KCL 3.x ShardRecordProcessor that processes stock trade records.
 * One instance is created per shard.
 */
public class StockTradeProcessor implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(StockTradeProcessor.class);

    // Checkpoint interval: balances DynamoDB write cost vs. duplicate processing on restart.
    //   Throughput / Cost:  60_000–120_000 (fewer DDB writes, more reprocessing after failure)
    //   Latency:            5_000–15_000   (less reprocessing, more DDB writes)
    private static final long CHECKPOINT_INTERVAL_MS = 60_000;

    private String shardId;
    private long lastCheckpointTimeMillis;

    @Override
    public void initialize(InitializationInput input) {
        this.shardId = input.shardId();
        this.lastCheckpointTimeMillis = System.currentTimeMillis();
        log.info("Initialized processor for shard: {}", shardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        input.records().forEach(record -> {
            String data = StandardCharsets.UTF_8.decode(record.data()).toString();
            log.info("Shard {}: {}", shardId, data);
        });

        // Periodic checkpoint: avoids a DDB write on every batch while keeping the
        // reprocessing window bounded. Adjust CHECKPOINT_INTERVAL_MS for your workload.
        long now = System.currentTimeMillis();
        if (now - lastCheckpointTimeMillis >= CHECKPOINT_INTERVAL_MS) {
            try {
                input.checkpointer().checkpoint();
                lastCheckpointTimeMillis = now;
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Error checkpointing on shard {}: {}", shardId, e.getMessage());
            }
        }
    }

    @Override
    public void leaseLost(LeaseLostInput input) {
        // Lease was taken by another worker — no checkpoint possible here
        log.info("Lease lost for shard: {}", shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput input) {
        // Shard has been closed (e.g. after a split/merge) — must checkpoint at SHARD_END
        try {
            input.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Error checkpointing at shard end for {}: {}", shardId, e.getMessage());
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        // KCL 3.x graceful lease handoff — checkpoint before handing off to another worker
        try {
            input.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Error checkpointing at shutdown for shard {}: {}", shardId, e.getMessage());
        }
    }
}
