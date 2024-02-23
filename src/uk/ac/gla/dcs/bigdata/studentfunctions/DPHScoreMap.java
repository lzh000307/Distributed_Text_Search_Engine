package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ResultWithQuery;

import java.util.*;

public class DPHScoreMap implements FlatMapFunction<NewsArticleProcessed, ResultWithQuery>{
    private Broadcast<Long> broadcastedTotalArticles;
    private Broadcast<Long> broadcastedTotalLength;
    private Broadcast<Map> broadcastedQueryTermFrequencyMap;
    private Broadcast<List<Query>> broadcastedQueryList;


    public DPHScoreMap(
            Broadcast<Long> broadcastedTotalArticles,
            Broadcast<Long> broadcastedTotalLength,
            Broadcast<Map> broadcastedQueryTermFrequencyMap,
            Broadcast<List<Query>> broadcastedQueryList){
        this.broadcastedQueryTermFrequencyMap = broadcastedQueryTermFrequencyMap;
        this.broadcastedTotalArticles = broadcastedTotalArticles;
        this.broadcastedTotalLength = broadcastedTotalLength;
        this.broadcastedQueryList = broadcastedQueryList;
    }

    @Override
    public Iterator<ResultWithQuery> call(NewsArticleProcessed newsArticleProcessed) throws Exception {
        List<ResultWithQuery> result = new ArrayList<>();
        // calculate the DPH score for each query term
        Map<String, Double> scores = calDPHScore(newsArticleProcessed);
        // sum up scores of queryterm according to query
        // get query
        List<Query> queryList = broadcastedQueryList.getValue();
        for(Query query : queryList){
            double score = 0;
            int queryTermSize = query.getQueryTerms().size();
            for(int i = 0; i < queryTermSize; i++){
                // if exists, add the score
                if(scores.containsKey(query.getQueryTerms().get(i))){
                    score += scores.get(query.getQueryTerms().get(i));
                }
            }
            if(score > 0.0) {
                result.add(new ResultWithQuery(
                        new RankedResult(
                                newsArticleProcessed.getId(),
                                newsArticleProcessed.getNewsArticle(),
                                score),
                        query));
            }
        }
//        System.out.println("Query: " + queryWithArticle.getQuery().getOriginalQuery() + " Score: " + score);
        return result.iterator();
    }

    /**
     * Calculate the DPH score for a given queryTerm and article
     * @author Zhenghao LIN
     * @param nap
     * @return Map<String, Double> scores, String is the queryTerm, Double is the score
     */
    private Map<String, Double> calDPHScore(NewsArticleProcessed nap){
        Long totalDocsInCorpus = broadcastedTotalArticles.getValue();
        Long currentDocumentLength = broadcastedTotalLength.getValue();
        double averageDocumentLengthInCorpus = currentDocumentLength / totalDocsInCorpus;
        Map<String, Long> totalTermFrequencyInCorpusMap = broadcastedQueryTermFrequencyMap.getValue();
//        System.out.println("totalTermFrequencyInCorpusMap: " + totalTermFrequencyInCorpusMap);
        Map<String, Double> scores = new HashMap<>();
        Map<String, Long> termFrequencyInCurrentDocumentMap = nap.getQueryTermFrequency();
                // calculate the score for each query term
        for(String queryTerm : termFrequencyInCurrentDocumentMap.keySet()){
            short termFrequencyInCurrentDocument = Long.valueOf(termFrequencyInCurrentDocumentMap.get(queryTerm)).shortValue();
//            int totalTermFrequencyInCorpus = totalTermFrequencyInCorpusMap.get(queryTerm);
            double score = DPHScorer.getDPHScore(
                    termFrequencyInCurrentDocument,
                    Long.valueOf(totalTermFrequencyInCorpusMap.get(queryTerm)).intValue(),
                    nap.getArticleLength(),
                    averageDocumentLengthInCorpus,
                    totalDocsInCorpus);
            scores.put(queryTerm, score);
//            if(nap.getId().equals("8c54645a-4de4-11e1-970f-5aedabc3a02c")){
//                System.out.println("queryTerm: " + queryTerm);
//                System.out.println("termFrequencyInCurrentDocument: " + termFrequencyInCurrentDocument);
//                System.out.println("totalTermFrequencyInCorpus: " + Long.valueOf(totalTermFrequencyInCorpusMap.get(queryTerm)).intValue());
//                System.out.println("articleLength: " + nap.getArticleLength());
//                System.out.println("averageDocumentLengthInCorpus: " + averageDocumentLengthInCorpus);
//                System.out.println("totalDocsInCorpus: " + totalDocsInCorpus);
//                System.out.println("score: " + score);
//            }
//            if(nap.getId().equals("5dbbd4e0-5297-11e1-bd4f-8a7d53f6d6c2")){
//                System.out.println("queryTerm: " + queryTerm);
//                System.out.println("termFrequencyInCurrentDocument: " + termFrequencyInCurrentDocument);
//                System.out.println("totalTermFrequencyInCorpus: " + Long.valueOf(totalTermFrequencyInCorpusMap.get(queryTerm)).intValue());
//                System.out.println("articleLength: " + nap.getArticleLength());
//                System.out.println("averageDocumentLengthInCorpus: " + averageDocumentLengthInCorpus);
//                System.out.println("totalDocsInCorpus: " + totalDocsInCorpus);
//                System.out.println("score: " + score);
//            }
        }
//        if(nap.getId().equals("985fe83c-45dd-11e1-b0ef-752cfddb1b51")){
//            System.out.println("Fannie and Freddie don’t deserve blame for bubble");
//            System.out.println("termFrequencyInCurrentDocumentMap: " + termFrequencyInCurrentDocumentMap);
//            System.out.println("scores: " + scores);
            // print dph parameters
//        }
        return scores;
    }

}
