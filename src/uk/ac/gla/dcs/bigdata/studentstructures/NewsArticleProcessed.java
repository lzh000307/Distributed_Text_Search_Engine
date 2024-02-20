package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.hadoop.fs.shell.Count;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NewsArticleProcessed implements Serializable {

    private String id; //unique article identifier
    private List<String> title; //article title
    private  List<String> contents; //article contents
    private int articleLength; //article's length
    private Map<String, Long> wordCount; //word frequency
    private Map<String, Long> queryTermFrequency; //query term frequency
    private boolean hitQueryTerms; //whether the article contains the query terms
    private NewsArticle newsArticle;

    public boolean isHitQueryTerms() {
        return hitQueryTerms;
    }

    public void setHitQueryTerms(boolean hitQueryTerms) {
        this.hitQueryTerms = hitQueryTerms;
    }

    public Map<String, Long> getQueryTermFrequency() {
        return queryTermFrequency;
    }

    public void setQueryTermFrequency(Map<String, Long> queryTermFrequency) {
        this.queryTermFrequency = queryTermFrequency;
    }

    public Map<String, Long> getWordCount() {
        return wordCount;
    }

    public void setWordCount(Map<String, Long> wordCount) {
        this.wordCount = wordCount;
    }
    public NewsArticleProcessed() {
    }

    public NewsArticleProcessed(String id, List<String> title, List<String> contents, int articleLength, Map<String, Long> wordCount, Map<String, Long> queryTermFrequency, boolean hitQueryTerms, NewsArticle newsArticle) {
        this.id = id;
        this.title = title;
        this.contents = contents;
        this.articleLength = articleLength;
        this.wordCount = wordCount;
        this.queryTermFrequency = queryTermFrequency;
        this.hitQueryTerms = hitQueryTerms;
        this.newsArticle = newsArticle;

    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public List<String> getContents() {
        return contents;
    }

    public void setContents(List<String> contents) {
        this.contents = contents;
    }

    public int getArticleLength() {
        return articleLength;
    }

    public void setArticleLength(int articleLength) {
        this.articleLength = articleLength;
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }
}
