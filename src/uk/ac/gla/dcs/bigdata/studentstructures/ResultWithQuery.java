package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class ResultWithQuery {
    private RankedResult rankedResult;
    private Query query;

    public ResultWithQuery(RankedResult rankedResult, Query query) {
        this.rankedResult = rankedResult;
        this.query = query;
    }

    public ResultWithQuery() {
    }
    public RankedResult getRankedResult() {
        return rankedResult;
    }

    public void setRankedResult(RankedResult rankedResult) {
        this.rankedResult = rankedResult;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
    public double getScore() {
        return this.rankedResult.getScore();
    }
}
