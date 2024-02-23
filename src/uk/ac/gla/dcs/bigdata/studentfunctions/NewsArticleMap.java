package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTermFrequencyAccumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewsArticleMap implements MapFunction<NewsArticle, NewsArticleProcessed> {

    private transient TextPreProcessor newsProcessor;
    private List<String> contentsProcessed;
    Broadcast<List<String>> broadcastedQueryTerms;
    private LongAccumulator totalArticlesAccumulator;
    private LongAccumulator totalLengthAccumulator;
    private QueryTermFrequencyAccumulator queryTermFrequencyAccumulator;
    public NewsArticleMap(Broadcast<List<String>> broadcastedQueryTerms, LongAccumulator totalArticles, LongAccumulator totalLength, QueryTermFrequencyAccumulator queryTermFrequencyAccumulator) {
        this.broadcastedQueryTerms = broadcastedQueryTerms;
        this.totalArticlesAccumulator = totalArticles;
        this.totalLengthAccumulator = totalLength;
        this.queryTermFrequencyAccumulator = queryTermFrequencyAccumulator;
    }
    private boolean hitQueryTerms = false;

    @Override
    public NewsArticleProcessed call(NewsArticle value) throws Exception {
        if(newsProcessor == null) {
            newsProcessor = new TextPreProcessor();
        }
        contentsProcessed = new ArrayList<String>();
        String id = value.getId();
        String title = value.getTitle();
//        String articleURL = value.getArticle_url();
//        String author = value.getAuthor();
//        long publishedDate = value.getPublished_date();
        List<ContentItem> contents = value.getContents();
//        String type = value.getType();
//        String source = value.getSource();

        int articleLength = 0;
        int paragraphNum = 0;
        for (ContentItem content : contents) {
            // if not null, get the subtype
            if(content != null && content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
                // check for and handle missing or null fields in the data.
                if(content.getContent().isBlank()) {
//                    System.out.println();
                    continue;
                }
                // only get the first 5 paragraphs
                if(paragraphNum++ == 5)
                    break;
//                System.out.println("Content: " + content.getContent());
                contentsProcessed.addAll(newsProcessor.process(content.getContent()));
            }
        }
        articleLength += contentsProcessed.size();
        //for the title:
        List<String> titleProcessed = newsProcessor.process(title);
        articleLength += titleProcessed.size();

        List<String> allWords = new ArrayList<>();
        allWords.addAll(titleProcessed);
        allWords.addAll(contentsProcessed);

        // Compute word count
        Map<String, Long> wordCount = new HashMap<>();
        Map<String, Long> queryTermFrequency = new HashMap<>();
        for(String word : allWords) {
            wordCount.put(word, wordCount.getOrDefault(word, 0L) + 1);
        }

        /**
         * There are a lot of words, so we need to minimize the functions executed in the for loop,
         * so we move the accumulator and queryTermFrequency to the later execution.
         */

        // Compute term frequency for query terms
        // CHECK if this article hit the query terms
        hitQueryTerms = false; // reset the flag

        for (String term : broadcastedQueryTerms.value()) {
            Long count = wordCount.getOrDefault(term, 0L);
            if(count > 0) {
                queryTermFrequency.put(term, count);
                hitQueryTerms = true;
            }
        }
        // When pruning, we can also prune those with empty title
        if(title == null || title.isBlank()) {
            hitQueryTerms = false;
        }

        // if queryTermFrequency's key is empty, print it
//        for(Map.Entry<String, Long> entry : queryTermFrequency.entrySet()) {
//            if(entry.getValue() > 0) {
////                System.out.println("Query Term Frequency: " + queryTermFrequency);
////                System.out.println("articleLength: " + articleLength + " | title: " + value.getTitle());
//            }
//        }


        // update accumulators
        totalArticlesAccumulator.add(1);
//        System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
        totalLengthAccumulator.add(articleLength);
//        System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());
        queryTermFrequencyAccumulator.add(queryTermFrequency);


        return new NewsArticleProcessed(id, titleProcessed, contentsProcessed, articleLength, wordCount, queryTermFrequency, hitQueryTerms, value);
    }
}
