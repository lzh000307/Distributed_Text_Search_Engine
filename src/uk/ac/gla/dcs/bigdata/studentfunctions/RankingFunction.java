package uk.ac.gla.dcs.bigdata.studentfunctions;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.MyQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.ResultWithQuery;

import java.util.*;

public class RankingFunction {
    public static List<DocumentRanking> rankDocuments(List<ResultWithQuery> resultWithQueryList, MyQueries myQueries){
        // convert to DocumentRanking
        Map<Query, List<RankedResult>> resultMap = new HashMap<>();
        for(ResultWithQuery resultWithQuery : resultWithQueryList) {
            Query query = myQueries.getQuery(resultWithQuery.getQuery());
            RankedResult rankedResult = resultWithQuery.getRankedResult();
            if(resultMap.containsKey(query)) {
                resultMap.get(query).add(rankedResult);
            } else {
                List<RankedResult> rankedResults = new ArrayList<>();
                rankedResults.add(rankedResult);
                resultMap.put(query, rankedResults);
            }
        }
        // convert to list
        List<DocumentRanking> documentRankings = new ArrayList<>();
        for(Map.Entry<Query, List<RankedResult>> entry : resultMap.entrySet()) {
            Query query = entry.getKey();
            List<RankedResult> rankedResults = entry.getValue();
            // sort the list
            Collections.sort(rankedResults);
            // reverse
            Collections.reverse(rankedResults);
            documentRankings.add(new DocumentRanking(query, rankedResults));
        }
        // now we have a List of DocumentRanking
        // compare similarity and find the top 10
        // new a output list
        List<DocumentRanking> output = new ArrayList<>();
        int diffrernce = 0; // the difference between the two documents rank
        for(DocumentRanking ranking : documentRankings){
            int resultNum = 0;
            int loopLength = ranking.getResults().size();
            // for speed up
            List<RankedResult> rr = ranking.getResults();
            List<RankedResult> newRR = new ArrayList<>();
            /**
             *  As a final stage, the ranking of documents for each query should be analysed to remove unneeded redundancy
             *  (near duplicate documents), if any pairs of documents are found where their titles have a textual distance
             *  (using a comparison function provided) less than 0.5 then you should only keep the most relevant of them
             *  (based on the DPH score). Note that there should be 10 documents returned for each query, even after
             *  redundancy filtering.
             */

            for(int i = 0; i < loopLength - 1; i++){
                if(newRR.size() == 10)
                    break;
                // not enough 10 results
                // compare similarity
                // current document title is t1
                String t1 = rr.get(i).getArticle().getTitle();
                boolean similarityFlag = false;
                // compare with previous documents
                for(RankedResult newRRInstance : newRR){
                    double distance = TextDistanceCalculator.similarity(t1, newRRInstance.getArticle().getTitle());
                    if (distance < 0.5) {
                        similarityFlag = true;
                        break;
                    }
                }
                if(similarityFlag){
                    continue;
                }
                //if not similar to any pairs of documents
                newRR.add(rr.get(i));
            }
//			if(rr.size() < 10){
//				System.out.println("WARNING: Query " + ranking.getQuery().getOriginalQuery() + " has less than 10 results");
//				// no need to add more results
//			}
            output.add(new DocumentRanking(ranking.getQuery(), newRR));
        }
        return output;
    }
}
