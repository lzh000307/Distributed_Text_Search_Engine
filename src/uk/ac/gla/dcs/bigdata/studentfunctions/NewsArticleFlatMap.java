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

    private transient TextPreProcessor newsProcessor;
    private Broadcast<List<String>> broadcastedQueryTerms;
    private LongAccumulator totalArticlesAccumulator;
    private LongAccumulator totalLengthAccumulator;
    private QueryTermFrequencyAccumulator queryTermFrequencyAccumulator;

    public NewsArticleFlatMap(Broadcast<List<String>> broadcastedQueryTerms, LongAccumulator totalArticles, LongAccumulator totalLength, QueryTermFrequencyAccumulator queryTermFrequencyAccumulator) {
        this.broadcastedQueryTerms = broadcastedQueryTerms;
        this.totalArticlesAccumulator = totalArticles;
        this.totalLengthAccumulator = totalLength;
        this.queryTermFrequencyAccumulator = queryTermFrequencyAccumulator;
    }

    @Override
    public Iterator<NewsArticleProcessed> call(NewsArticle value) throws Exception {
        List<NewsArticleProcessed> result = new ArrayList<>();
        if (newsProcessor == null) {
            newsProcessor = new TextPreProcessor();
        }

        List<String> contentsProcessed = new ArrayList<>();
        String id = value.getId();
        String title = value.getTitle();
        List<ContentItem> contents = value.getContents();

        int articleLength = 0;
        int paragraphNum = 0;
        for (ContentItem content : contents) {
            if (content != null && "paragraph".equals(content.getSubtype())) {
                if (content.getContent().isBlank()) {
                    continue;
                }
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


        totalArticlesAccumulator.add(1);
        totalLengthAccumulator.add(articleLength);
        queryTermFrequencyAccumulator.add(queryTermFrequency);

        if (title == null || title.isBlank() || !hitQueryTerms) {
            return result.iterator();
        }

        NewsArticleProcessed processedArticle = new NewsArticleProcessed(id, articleLength, queryTermFrequency, value);
        result.add(processedArticle);
        return result.iterator();
    }
}