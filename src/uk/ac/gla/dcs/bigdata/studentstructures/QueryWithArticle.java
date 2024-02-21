package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryWithArticle {
    private Query query;
    private NewsArticleProcessed newsArticleProcessed;
    private short frequency;

    public QueryWithArticle(Query query, NewsArticleProcessed newsArticleProcessed, short frequency) {
        this.query = query;
        this.newsArticleProcessed = newsArticleProcessed;
        this.frequency = frequency;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
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

}
