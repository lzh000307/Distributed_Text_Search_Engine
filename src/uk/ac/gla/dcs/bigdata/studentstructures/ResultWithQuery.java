package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class ResultWithQuery {
    private RankedResult rankedResult;
    private String query;

    public ResultWithQuery(RankedResult rankedResult, String query) {
        this.rankedResult = rankedResult;
        this.query = query;
    }

    public RankedResult getRankedResult() {
        return rankedResult;
    }

    public void setRankedResult(RankedResult rankedResult) {
        this.rankedResult = rankedResult;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
