package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class QueryTermFrequency implements Serializable {
    private String term;
    private Long frequency;

    public QueryTermFrequency() {
    }

    public QueryTermFrequency(String term, Long frequency) {
        this.term = term;
        this.frequency = frequency;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }
}
