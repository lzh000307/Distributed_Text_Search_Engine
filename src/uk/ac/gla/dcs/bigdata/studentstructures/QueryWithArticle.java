package uk.ac.gla.dcs.bigdata.studentstructures;

public class QueryWithArticle {
    private String queryTerm;
    private NewsArticleProcessed newsArticleProcessed;
    private short frequency;

    public String getQueryTerm() {
        return queryTerm;
    }
    public String getQuery() {
        return queryTerm;
    }

    public void setQueryTerm(String queryTerm) {
        this.queryTerm = queryTerm;
    }

    public NewsArticleProcessed getNewsArticleProcessed() {
        return newsArticleProcessed;
    }

    public void setNewsArticleProcessed(NewsArticleProcessed newsArticleProcessed) {
        this.newsArticleProcessed = newsArticleProcessed;
    }

    public short getFrequency() {
        return frequency;
    }

    public void setFrequency(short frequency) {
        this.frequency = frequency;
    }

    public QueryWithArticle(String queryTerm, NewsArticleProcessed newsArticleProcessed, short frequency) {
        this.queryTerm = queryTerm;
        this.newsArticleProcessed = newsArticleProcessed;
        this.frequency = frequency;
    }
}
