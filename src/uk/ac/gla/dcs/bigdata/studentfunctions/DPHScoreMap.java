package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ResultWithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DPHScoreMap implements FlatMapFunction<QueryWithArticle, ResultWithQuery>{
    private Broadcast<Long> broadcastedTotalArticles;
    private Broadcast<Long> broadcastedTotalLength;
    private Broadcast<Map> broadcastedQueryFrequencyMap;



    public DPHScoreMap(Broadcast<Long> broadcastedTotalArticles, Broadcast<Long> broadcastedTotalLength, Broadcast<Map> broadcastedQueryFrequencyMap){
        this.broadcastedQueryFrequencyMap = broadcastedQueryFrequencyMap;
        this.broadcastedTotalArticles = broadcastedTotalArticles;
        this.broadcastedTotalLength = broadcastedTotalLength;
    }

    @Override
    public Iterator<ResultWithQuery> call(NewsArticleProcessed newsArticleProcessed) throws Exception {
        List<ResultWithQuery> result = new ArrayList<>();
        Long totalArticles = broadcastedTotalArticles.getValue();
        Long totalLength = broadcastedTotalLength.getValue();
        double averageDocumentLengthInCorpus = totalLength / totalArticles;
        Map<Query, Integer> queryFrequencyMap = broadcastedQueryFrequencyMap.getValue();
//        Long frequency = (long) queryWithArticle.getFrequency();
//        int length = queryWithArticle.getNewsArticleProcessed().getArticleLength();
//        int queryFrequency = queryFrequencyMap.get(queryWithArticle.getQuery());
        double score = 0L;
//        for (String queryTerm : queryWithArticle.getNewsArticleProcessed().getQueryTermFrequency().keySet()) {
//            Long queryFrequency = queryFrequencyMap.get(queryTerm);
//            Long articleFrequency = queryWithArticle.getNewsArticleProcessed().getQueryTermFrequency().get(queryTerm);
//            score += Math.log((frequency + 1) / (queryFrequency + 1)) * (articleFrequency / length);
//        }
//        score += Math.log(totalArticles / totalLength);
        //print all
        score = DPHScorer.getDPHScore(newsArticleProcessed.getFrequency(),
                queryFrequencyMap.get(newsArticleProcessed.getQuery()),
                queryWithArticle.getNewsArticleProcessed().getArticleLength(),
                averageDocumentLengthInCorpus,
                totalArticles);
        result.add(new ResultWithQuery(
                new RankedResult(
                        queryWithArticle.getNewsArticleProcessed().getId(),
                        queryWithArticle.getNewsArticleProcessed().getNewsArticle(),
                        score),
                queryWithArticle.getQuery()));
//        System.out.println("Query: " + queryWithArticle.getQuery().getOriginalQuery() + " Score: " + score);
        return result.iterator();
    }

}
