package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

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

    public NewsArticleMap(Broadcast<List<String>> broadcastedQueryTerms, LongAccumulator totalArticles, LongAccumulator totalLength) {
        this.broadcastedQueryTerms = broadcastedQueryTerms;
        this.totalArticlesAccumulator = totalArticles;
        this.totalLengthAccumulator = totalLength;
    }

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
            if(content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
                // check for and handle missing or null fields in the data.
                if(content.getContent().isBlank()) {
                    System.out.println();
                    continue;
                }
                // only get the first 5 paragraphs
                if(++paragraphNum == 5)
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

        // 统计单词频率
        Map<String, Long> wordCount = new HashMap<>();
        for(String word : allWords) {
            wordCount.put(word, wordCount.getOrDefault(word, 0L) + 1);
        }

        // Compute term frequency for query terms
        Map<String, Long> queryTermFrequency = new HashMap<>();
        for (String term : broadcastedQueryTerms.value()) {
            Long count = wordCount.getOrDefault(term, 0L);
            queryTermFrequency.put(term, count);
        }



        // 更新累加器
        totalArticlesAccumulator.add(1);
//        System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
        totalLengthAccumulator.add(articleLength);
//        System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());



        return new NewsArticleProcessed(id, titleProcessed, contentsProcessed, articleLength, wordCount, queryTermFrequency);
    }
}
