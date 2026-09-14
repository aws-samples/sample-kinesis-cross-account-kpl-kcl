// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.samples.kpl;

import java.util.Random;

/**
 * Simple stock trade model. Ticker is used as the KPL partition key so that
 * all trades for the same stock land on the same shard (and are processed in order).
 */
public class StockTrade {

    private static final String[] TICKERS = {"AMZN", "GOOG", "MSFT", "AAPL", "META", "NFLX", "TSLA", "NVDA"};
    private static final String[] TRADE_TYPES = {"BUY", "SELL"};
    private static final Random RANDOM = new Random();

    private final String ticker;
    private final String tradeType;
    private final double price;
    private final int quantity;
    private final long timestamp;

    public StockTrade(String ticker, String tradeType, double price, int quantity) {
        this.ticker = ticker;
        this.tradeType = tradeType;
        this.price = price;
        this.quantity = quantity;
        this.timestamp = System.currentTimeMillis();
    }

    public static StockTrade random() {
        String ticker = TICKERS[RANDOM.nextInt(TICKERS.length)];
        String tradeType = TRADE_TYPES[RANDOM.nextInt(TRADE_TYPES.length)];
        double price = 10 + RANDOM.nextDouble() * 990; // $10 - $1000
        int quantity = 1 + RANDOM.nextInt(1000);
        return new StockTrade(ticker, tradeType, price, quantity);
    }

    public String getTicker() { return ticker; }

    public String toJson() {
        return String.format(
                "{\"ticker\":\"%s\",\"tradeType\":\"%s\",\"price\":%.2f,\"quantity\":%d,\"timestamp\":%d}",
                ticker, tradeType, price, quantity, timestamp);
    }
}
