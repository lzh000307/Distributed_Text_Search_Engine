package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.util.AccumulatorV2;
import java.util.HashMap;
import java.util.Map;

public class QueryTermFrequencyAccumulator extends AccumulatorV2<Map<String, Long>, Map<String, Long>> {
    private Map<String, Long> termFrequency;

    public QueryTermFrequencyAccumulator() {
        this.termFrequency = new HashMap<>();
    }

    @Override
    public boolean isZero() {
        return termFrequency.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<String, Long>, Map<String, Long>> copy() {
        QueryTermFrequencyAccumulator newAcc = new QueryTermFrequencyAccumulator();
        newAcc.termFrequency.putAll(this.termFrequency);
        return newAcc;
    }

    @Override
    public void reset() {
        termFrequency.clear();
//        termFrequency = new HashMap<>();
//        System.out.println("CLEAR: termFrequency: " + termFrequency);
    }

    @Override
    public void add(Map<String, Long> v) {
        // if not 0
        for (Map.Entry<String, Long> entry : v.entrySet()) {
            if(entry.getValue() != 0L) {
                termFrequency.merge(entry.getKey(), entry.getValue(), Long::sum); // merge the value of the same key
//                System.out.println("Key: " + entry.getKey() + " Value: " + entry.getValue() + " termFrequency: " + termFrequency.get(entry.getKey()));
            }
//            termFrequency.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    @Override
    public void merge(AccumulatorV2<Map<String, Long>, Map<String, Long>> other) {
        if (other instanceof QueryTermFrequencyAccumulator) {
            for (Map.Entry<String, Long> entry : other.value().entrySet()) {
                termFrequency.merge(entry.getKey(), entry.getValue(), Long::sum);
//                System.out.println("MERGE: " + termFrequency);
            }
        }
    }

    @Override
    public Map<String, Long> value() {
        return termFrequency;
    }
}