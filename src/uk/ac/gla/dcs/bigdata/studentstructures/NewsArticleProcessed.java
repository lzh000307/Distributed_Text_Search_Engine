package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewsArticleProcessed implements Serializable {

    private String id; //unique article identifier
    private int articleLength; //article's length
    private List<QueryTermFrequency> queryTermFrequency; //query term frequency
    private NewsArticle newsArticle;

    public void setId(String id) {
        this.id = id;
    }

    public void setArticleLength(int articleLength) {
        this.articleLength = articleLength;
    }

    public List<QueryTermFrequency> getQueryTermFrequency() {
        return queryTermFrequency;
    }

    public void setQueryTermFrequency(List<QueryTermFrequency> queryTermFrequency) {
        this.queryTermFrequency = queryTermFrequency;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public Map<String, Long> getQueryTermFrequencyMap() {
        Map<String, Long> queryTermFrequencyMap = new HashMap<>();
        for (QueryTermFrequency qtf : queryTermFrequency) {
            queryTermFrequencyMap.put(qtf.getTerm(), qtf.getFrequency());
        }
        return queryTermFrequencyMap;
    }

    public NewsArticleProcessed() {
    }

    public NewsArticleProcessed(String id, int articleLength, Map<String, Long> queryTermFrequency, NewsArticle newsArticle) {
        this.id = id;
        this.articleLength = articleLength;
        this.queryTermFrequency = new ArrayList<>();
        for (Map.Entry<String, Long> entry : queryTermFrequency.entrySet()) {
            this.queryTermFrequency.add(new QueryTermFrequency(entry.getKey(), entry.getValue()));
            this.newsArticle = newsArticle;
        }
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
