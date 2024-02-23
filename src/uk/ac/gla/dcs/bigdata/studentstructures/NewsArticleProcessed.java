package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.hadoop.fs.shell.Count;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewsArticleProcessed implements Serializable {

    private String id; //unique article identifier
    private int articleLength; //article's length
    private Map<String, Long> queryTermFrequency; //query term frequency
    private NewsArticle newsArticle;

    public Map<String, Long> getQueryTermFrequency() {
        return queryTermFrequency;
    }

    public NewsArticleProcessed() {
    }

    public NewsArticleProcessed(String id, int articleLength, Map<String, Long> queryTermFrequency, NewsArticle newsArticle) {
        this.id = id;
        this.articleLength = articleLength;
        this.queryTermFrequency = queryTermFrequency;
        this.newsArticle = newsArticle;
    }


    public String getId() {
        return id;
    }


    public int getArticleLength() {
        return articleLength;
    }


    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

}
