package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import java.util.List;

public class ResultList {
    private String query;
    private List<RankedResult> rankedResultList;

    public ResultList(String query, List<RankedResult> rankedResultList) {
        this.query = query;
        this.rankedResultList = rankedResultList;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<RankedResult> getRankedResultList() {
        return rankedResultList;
    }

    public void setRankedResultList(List<RankedResult> rankedResultList) {
        this.rankedResultList = rankedResultList;
    }
}
