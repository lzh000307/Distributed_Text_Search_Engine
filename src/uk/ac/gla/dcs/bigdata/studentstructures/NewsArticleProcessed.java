package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.hadoop.fs.shell.Count;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NewsArticleProcessed implements Serializable {

    private String id; //unique article identifier
    private String articleURL; //article url
    private List<String> title; //article title
    private String author; //article author
    private long publishedDate; // publication date as a unix timestamp (ms)
    private  List<String> contents; //article contents
    private String type; //article type
    private String source; //article source
    private int articleLength; //article's length
    private Map<String, Long> wordCount; //word frequency

    public Map<String, Long> getWordCount() {
        return wordCount;
    }

    public void setWordCount(Map<String, Long> wordCount) {
        this.wordCount = wordCount;
    }

    public NewsArticleProcessed(String id, List<String> title, List<String> contents, int articleLength, Map<String, Long> wordCount) {
        this.id = id;
        this.title = title;
        this.contents = contents;
        this.articleLength = articleLength;
        this.wordCount = wordCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getArticleURL() {
        return articleURL;
    }

    public void setArticleURL(String articleURL) {
        this.articleURL = articleURL;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public long getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(long publishedDate) {
        this.publishedDate = publishedDate;
    }

    public List<String> getContents() {
        return contents;
    }

    public void setContents(List<String> contents) {
        this.contents = contents;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getArticleLength() {
        return articleLength;
    }

    public void setArticleLength(int articleLength) {
        this.articleLength = articleLength;
    }
}
