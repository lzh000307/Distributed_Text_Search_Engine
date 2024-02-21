package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyQueries {
    List<Query> queries;
    Map<String, Query> queryMap;

    public MyQueries(List<Query> queries) {
        this.queries = queries;
        queryMap = new HashMap<>();
        for (Query query : queries) {
            queryMap.put(query.getOriginalQuery(), query);
        }
    }
    public boolean ifExist(Query query) {
        return queryMap.containsKey(query.getOriginalQuery());
    }
    public Query getQuery(String query) {
        return queryMap.get(query);
    }
    public Query getQuery(Query query){
        return queryMap.get(query.getOriginalQuery());
    }
}
