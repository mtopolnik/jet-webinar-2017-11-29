/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.core.IMap;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.map.journal.EventJournalMapEvent;
import datamodel.Broker;
import datamodel.Product;
import datamodel.Trade;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.JoinClause.joinMapEntries;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class Sample02 {
    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final int PRODUCT_ID_BASE = 21;
    private static final int BROKER_ID_BASE = 31;
    private static final int PRODUCT_BROKER_COUNT = 4;

    private final JetInstance jet;

    private Sample02(JetInstance jet) {
        this.jet = jet;
    }

    // Demonstrates the use of the simple, fully typesafe API to construct
    // a hash join with up to two enriching streams
    private static Pipeline hashJoinPipeline() {
        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        ComputeStage<Trade> trades = p.drawFrom(Sources.<Object, Trade, Trade>mapJournal(TRADES,
                alwaysTrue(), EventJournalMapEvent::getNewValue, true));

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>map(PRODUCTS));
        ComputeStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>map(BROKERS));

        // Join the trade stream with the product and broker streams
        ComputeStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId)
        );

        // Send the joined tuples to the logging sink
        joined.drainTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        // Lower operation timeout to speed up job cancellation
        System.setProperty("hazelcast.operation.call.timeout.millis", "1000");

        JetInstance jet = Jet.newJetClient();
        new Sample02(jet).go();
    }

    private void go() throws Exception {
        prepareEnrichingData();
        EventGenerator eventGenerator = new EventGenerator(jet.getMap(TRADES));
        eventGenerator.start();
        try {
            JobConfig cfg = new JobConfig().addClass(Sample02.class);
            Job job = jet.newJob(hashJoinPipeline(), cfg);
            eventGenerator.generateEventsForFiveSeconds();
            job.cancel();
            Thread.sleep(2000);
        } finally {
            eventGenerator.shutdown();
            Jet.shutdownAll();
        }
    }

    private void prepareEnrichingData() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);

        String[] prodNames = {
                "GOOGL", "AAPL", "FACB", "FB"
        };
        String[] brokNames = {
                "Henry", "William", "Ginger", "Esther"
        };

        int productId = PRODUCT_ID_BASE;
        int brokerId = BROKER_ID_BASE;
        for (int i = 0; i < PRODUCT_BROKER_COUNT; i++) {
            Product prod = new Product(productId, prodNames[i]);
            Broker brok = new Broker(brokerId, brokNames[i]);
            productMap.put(productId, prod);
            brokerMap.put(brokerId, brok);
            productId++;
            brokerId++;
        }
        printImap(productMap);
        printImap(brokerMap);
    }

    private static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.out.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.out.println(sb);
    }

    private static class EventGenerator extends Thread {
        private volatile boolean enabled;
        private volatile boolean keepRunning = true;

        private final IMap<Object, Trade> trades;

        EventGenerator(IMap<Object, Trade> trades) {
            this.trades = trades;
        }

        @Override
        public void run() {
            Random rnd = ThreadLocalRandom.current();
            int tradeId = 1;
            while (keepRunning) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(50));
                if (!enabled) {
                    continue;
                }
                Trade trad = new Trade(tradeId,
                        PRODUCT_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT),
                        BROKER_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT));
                trades.put(42, trad);
                System.out.println("Publish: " + trad);
                tradeId++;
            }
        }

        void generateEventsForFiveSeconds() throws InterruptedException {
            enabled = true;
            System.out.println("\n\nGenerating trade events\n");
            Thread.sleep(5000);
            System.out.println("\n\nStopped trade events\n");
            enabled = false;
        }

        void shutdown() {
            keepRunning = false;
        }
    }
}
