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
package webinar;

import com.hazelcast.jet.*;
import com.hazelcast.jet.config.JobConfig;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class Sample01 {
    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("text"))
         .flatMap(word -> traverseArray(word.toLowerCase().split("\\W+")))
         .filter(word -> !word.isEmpty())
         .groupBy(wholeItem(), counting())
         .drainTo(Sinks.map("counts"));

        JetInstance jet = Jet.newJetInstance();
        List<String> text = jet.getList("text");
        Map<String, Long> counts = jet.getMap("counts");
        try {
            text.add("hello world hello hello world");
            text.add("world world hello world");

            jet.newJob(p, new JobConfig().addClass(Sample01.class))
               .join();

            System.out.println("Count of hello: " + counts.get("hello"));
            System.out.println("Count of world: " + counts.get("world"));
        } finally {
            text.clear();
            counts.clear();
            Jet.shutdownAll();
        }
    }
}
