package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import java.util.ArrayList;
import java.util.List;

public class SimilarityFilter implements MapFunction<DocumentRanking, DocumentRanking> {
    
    // private static final long serialVersionUID = 0710;

    @Override
    public DocumentRanking call(DocumentRanking value) throws Exception {
        List<RankedResult> initialResults = value.getResults();
        List<RankedResult> filteredAndRankedResults = new ArrayList<>();

        // 逻辑：过滤相似文档并保留评分前十的文档
        for (int i = 0; i < initialResults.size(); i++) {
            RankedResult currentResult = initialResults.get(i);
            boolean isUnique = true;

            for (int j = 0; j < filteredAndRankedResults.size(); j++) {
                RankedResult comparedResult = filteredAndRankedResults.get(j);
                double distance = TextDistanceCalculator.similarity(currentResult.getArticle().getTitle(), comparedResult.getArticle().getTitle());
                
                if (distance < 0.5) {
                    isUnique = false;
                    break;
                }
            }

            if (isUnique) {
                filteredAndRankedResults.add(currentResult);
            }

            // 保证结果不超过10个
            if (filteredAndRankedResults.size() == 10) {
                break;
            }
        }

        return new DocumentRanking(value.getQuery(), filteredAndRankedResults);
    }
}
