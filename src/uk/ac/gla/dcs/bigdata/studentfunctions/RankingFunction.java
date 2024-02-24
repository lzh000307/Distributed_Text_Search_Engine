package uk.ac.gla.dcs.bigdata.studentfunctions;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.MyQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.ResultWithQuery;

import java.util.*;

public class RankingFunction {
    /**
     * Static method to rank documents return the top 10 documents for each query without redundancy
     * @param resultWithQueryList list of RankedResult associated with queries
     * @param myQueries a Map of queries
     * @return a list of DocumentRanking, the top 10 documents for each query
     */
    public static List<DocumentRanking> rankDocuments(List<ResultWithQuery> resultWithQueryList, MyQueries myQueries){

        // Initialization and grouping of ranked results by query.
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

        // Conversion of the resultMap to a list of DocumentRanking objects.
        List<DocumentRanking> documentRankings = new ArrayList<>();
        for(Map.Entry<Query, List<RankedResult>> entry : resultMap.entrySet()) {
            Query query = entry.getKey();
            List<RankedResult> rankedResults = entry.getValue();

            // Each document ranking is sorted and reversed to ensure the highest-ranked documents are first.
            Collections.sort(rankedResults);
            Collections.reverse(rankedResults);
            documentRankings.add(new DocumentRanking(query, rankedResults));
        }

        // Final stage of ranking involves filtering out near-duplicate documents based on title similarity.
        List<DocumentRanking> output = new ArrayList<>();
        for(DocumentRanking ranking : documentRankings){
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
                // compare similarity
                // current document title is t1
                String t1 = rr.get(i).getArticle().getTitle();
                boolean similarityFlag = false;
                // compare with previous documents in newRR
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
                // if not similar to any pairs of documents, add it to newRR
                newRR.add(rr.get(i));
            }
            /**
             * After discussing with professor,
             * we agreed that it is almost impossible to search for less than 10 news items in the large data set
             * Therefore, we didn't do a random sampling of irrelevant news when the results were less than 10.
             */
//			if(rr.size() < 10){
//				System.out.println("WARNING: Query " + ranking.getQuery().getOriginalQuery() + " has less than 10 results");
//				// no need to add more results
//			}
            output.add(new DocumentRanking(ranking.getQuery(), newRR));
        }
        return output;
    }
}
