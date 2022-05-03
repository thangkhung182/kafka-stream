/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trungnguyen.kafkastream.collectors;


import com.trungnguyen.kafkastream.model.StockTransaction;

public class StockTransactionCollector {

    private double amount;
    private String tickerSymbol;
    private int sharesPurchased;
    private int sharesSold;

    public StockTransactionCollector add(StockTransaction transaction){
        if(tickerSymbol == null){
            tickerSymbol = transaction.getSymbol();
        }

        this.amount += transaction.getSharePrice();
        if(transaction.getSector().equalsIgnoreCase("purchase")){
            this.sharesPurchased += transaction.getShares();
        } else{
            this.sharesSold += transaction.getShares();
        }
        return this;
    }

    @Override
    public String toString() {
        return "StockTransactionCollector{" +
                "amount=" + amount +
                ", tickerSymbol='" + tickerSymbol + '\'' +
                ", sharesPurchased=" + sharesPurchased +
                ", sharesSold=" + sharesSold +
                '}';
    }
}