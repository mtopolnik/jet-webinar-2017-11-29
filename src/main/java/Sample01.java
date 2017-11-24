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

        try {
            JetInstance jet = Jet.newJetClient();
            List<String> text = jet.getList("text");
            text.add("hello world hello hello world");
            text.add("world world hello world");

            jet.newJob(p, new JobConfig().addClass(Sample01.class))
               .join();

            Map<String, Long> counts = jet.getMap("counts");
            System.out.println("Count of hello: " + counts.get("hello"));
            System.out.println("Count of world: " + counts.get("world"));
        } finally {
            Jet.shutdownAll();
        }
    }
}
