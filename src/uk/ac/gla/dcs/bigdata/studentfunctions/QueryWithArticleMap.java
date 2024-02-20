package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithArticle;

import java.awt.datatransfer.FlavorMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryWithArticleMap implements FlatMapFunction<NewsArticleProcessed, QueryWithArticle> {

    private Broadcast<List<Query>> broadcastedQueries;
    public QueryWithArticleMap(Broadcast<List<Query>> broadcastedQueries){
        this.broadcastedQueries = broadcastedQueries;
    }

    @Override
    public Iterator<QueryWithArticle> call(NewsArticleProcessed newsArticleProcessed) throws Exception{
//        System.out.println(newsArticleProcessed);
        List<QueryWithArticle> result = new ArrayList<>();
        for (Query query : broadcastedQueries.getValue()) {
            Long count = 0L;
            int numOfTerms = 0;
            // for each query, calculate the average frequency of query term in the article
            int length = query.getQueryTerms().size();
            for(int i=0; i<length; i++) {
                count += newsArticleProcessed.getQueryTermFrequency().getOrDefault(query.getQueryTerms().get(i), 0L);
                numOfTerms += query.getQueryTermCounts()[i];
            }
            if(count != 0){
                short countShort = Long.valueOf(count).shortValue();
//                short countShort = (short) (count/numOfTerms);
                QueryWithArticle queryWithArticle = new QueryWithArticle(query.getOriginalQuery(), newsArticleProcessed, countShort);
                result.add(queryWithArticle);
            }
        }
        //iterate
        return result.iterator();
    }
}
