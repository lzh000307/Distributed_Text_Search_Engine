package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTermFrequencyAccumulator;

import java.util.*;

public class NewsArticleFlatMap implements FlatMapFunction<NewsArticle, NewsArticleProcessed> {
    // Class variables for processing and accumulating statistics about the articles.
    private transient TextPreProcessor newsProcessor;
    private final Broadcast<List<String>> broadcastedQueryTerms;
    private final LongAccumulator totalArticlesAccumulator;
    private final LongAccumulator totalLengthAccumulator;
    private final QueryTermFrequencyAccumulator queryTermFrequencyAccumulator;

    public NewsArticleFlatMap(Broadcast<List<String>> broadcastedQueryTerms, LongAccumulator totalArticles, LongAccumulator totalLength, QueryTermFrequencyAccumulator queryTermFrequencyAccumulator) {
        this.broadcastedQueryTerms = broadcastedQueryTerms;
        this.totalArticlesAccumulator = totalArticles;
        this.totalLengthAccumulator = totalLength;
        this.queryTermFrequencyAccumulator = queryTermFrequencyAccumulator;
    }

    /**
     * Method to process each news article, filtering and transforming it for further analysis.
     * @param value instance of NewsArticle
     * @return list of NewsArticleProcessed that only hit the query terms and have a title.
     */
    @Override
    public Iterator<NewsArticleProcessed> call(NewsArticle value) {
        List<NewsArticleProcessed> result = new ArrayList<>();
        // Initialize the text processor if it's not already.
        if (newsProcessor == null) {
            newsProcessor = new TextPreProcessor();
        }

        List<String> contentsProcessed = new ArrayList<>();
        String id = value.getId();

        // Preprocessing content and title, calculating total length and identifying query terms.
        String title = value.getTitle();
        List<ContentItem> contents = value.getContents();
        int articleLength = 0;
        int paragraphNum = 0;
        for (ContentItem content : contents) {
            if (content != null && "paragraph".equals(content.getSubtype())) {
                /**
                 * If you need to filter out blank paragraphs, uncomment the following code:
                 */
//                if (content.getContent().isBlank()) {
//                    continue;
//                }
                if (paragraphNum++ == 5) break;
                contentsProcessed.addAll(newsProcessor.process(content.getContent()));
            }
        }
        articleLength += contentsProcessed.size();
        List<String> titleProcessed = newsProcessor.process(title);
        articleLength += titleProcessed.size();

        List<String> allWords = new ArrayList<>(titleProcessed);
        allWords.addAll(contentsProcessed);

        Map<String, Long> wordCount = new HashMap<>();
        for (String word : allWords) {
            wordCount.put(word, wordCount.getOrDefault(word, 0L) + 1);
        }

        Map<String, Long> queryTermFrequency = new HashMap<>();
        boolean hitQueryTerms = false;
        for (String term : broadcastedQueryTerms.value()) {
            Long count = wordCount.getOrDefault(term, 0L);
            if (count > 0) {
                queryTermFrequency.put(term, count);
                hitQueryTerms = true;
            }
        }

        // Accumulators are used to calculate DPH Score.
        totalArticlesAccumulator.add(1);
        totalLengthAccumulator.add(articleLength);
        queryTermFrequencyAccumulator.add(queryTermFrequency);

        // A processed article is only added to the result if it contains query terms and title.
        // Because articles without a title cannot be checked the similarity and ranked.
        if (title == null || title.isBlank() || !hitQueryTerms) {
            return result.iterator();
        }
        NewsArticleProcessed processedArticle = new NewsArticleProcessed(id, articleLength, queryTermFrequency, value);
        result.add(processedArticle);
        return result.iterator();
    }
}