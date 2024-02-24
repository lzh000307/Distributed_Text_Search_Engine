package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.ResultWithQuery;

import java.util.*;

public class DPHScoreMap implements FlatMapFunction<NewsArticleProcessed, ResultWithQuery>{
    private final Broadcast<Long> broadcastedTotalArticles;
    private final Broadcast<Long> broadcastedTotalLength;
    private final Broadcast<Map<String, Long>> broadcastedQueryTermFrequencyMap;
    private final Broadcast<List<Query>> broadcastedQueryList;


    public DPHScoreMap(
            Broadcast<Long> broadcastedTotalArticles,
            Broadcast<Long> broadcastedTotalLength,
            Broadcast<Map<String, Long>> broadcastedQueryTermFrequencyMap,
            Broadcast<List<Query>> broadcastedQueryList){
        this.broadcastedQueryTermFrequencyMap = broadcastedQueryTermFrequencyMap;
        this.broadcastedTotalArticles = broadcastedTotalArticles;
        this.broadcastedTotalLength = broadcastedTotalLength;
        this.broadcastedQueryList = broadcastedQueryList;
    }

    /**
     * This function calculates the DPH score for each query term and returns a list of ResultWithQuery
     * Each ResultWithQuery contains a RankedResult (which contains a DPH score) and a Query
     * @param newsArticleProcessed instance of NewsArticleProcessed
     * @return list of ResultWithQuery
     */
    @Override
    public Iterator<ResultWithQuery> call(NewsArticleProcessed newsArticleProcessed) {
        List<ResultWithQuery> result = new ArrayList<>();
        // Calculate DPH scores for each query term in the article
        Map<String, Double> scores = calDPHScore(newsArticleProcessed);
        // Aggregate scores for each query and create a ResultWithQuery object
        List<Query> queryList = broadcastedQueryList.getValue();
        for(Query query : queryList){
            double score = 0;
            // To avoid the overhead of calling query.getQueryTerms().size() multiple times
            int queryTermSize = query.getQueryTerms().size();
            // Sum up scores for terms present in the query
            for(int i = 0; i < queryTermSize; i++){
                // if exists, add the score
                if(scores.containsKey(query.getQueryTerms().get(i))){
                    score += scores.get(query.getQueryTerms().get(i));
                }
            }
            // if the score is greater than 0, add to the result list
            // if the score is 0, it means the article does not hit the current query
            if(score > 0.0) {
                result.add(new ResultWithQuery(
                        new RankedResult(
                                newsArticleProcessed.getId(),
                                newsArticleProcessed.getNewsArticle(),
                                score),
                        query));
            }
        }
        return result.iterator();
    }

    /**
     * Calculate the DPH score for a given queryTerm and article
     * @param nap is a NewsArticleProcessed instance
     * @return Map<String, Double> scores, String is the queryTerm, Double is the score
     */
    private Map<String, Double> calDPHScore(NewsArticleProcessed nap){
        Long totalDocsInCorpus = broadcastedTotalArticles.getValue();
        Long totalDocumentLength = broadcastedTotalLength.getValue();
        double averageDocumentLengthInCorpus = (double)totalDocumentLength / (double)totalDocsInCorpus;
        Map<String, Long> totalTermFrequencyInCorpusMap = broadcastedQueryTermFrequencyMap.getValue();
        Map<String, Double> scores = new HashMap<>();
        Map<String, Long> termFrequencyInCurrentDocumentMap = nap.getQueryTermFrequencyMap();

        // Calculation logic for DPH scores based on term frequencies and document statistics
        for(String queryTerm : termFrequencyInCurrentDocumentMap.keySet()){
            short termFrequencyInCurrentDocument = termFrequencyInCurrentDocumentMap.get(queryTerm).shortValue();
            double score = DPHScorer.getDPHScore(
                    termFrequencyInCurrentDocument,
                    totalTermFrequencyInCorpusMap.get(queryTerm).intValue(),
                    nap.getArticleLength(),
                    averageDocumentLengthInCorpus,
                    totalDocsInCorpus);
            scores.put(queryTerm, score);
        }
        return scores;
    }

}
