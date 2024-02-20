package uk.ac.gla.dcs.bigdata.studentstructures;

public class QueryFrequency {
    private String queryTerm;
    private short frequency;

    public QueryFrequency(String queryTerm, short frequency) {
        this.queryTerm = queryTerm;
        this.frequency = frequency;
    }

    public String getQueryTerm() {
        return queryTerm;
    }

    public short getFrequency() {
        return frequency;
    }
}
