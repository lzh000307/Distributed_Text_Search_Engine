package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

import java.util.ArrayList;
import java.util.List;

public class NewsArticleMap implements MapFunction<NewsArticle, NewsArticleProcessed> {

    private transient TextPreProcessor newsProcessor;
    private List<String> contentsProcessed;

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
        // find the first 5 paragraphs of the article

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
        return new NewsArticleProcessed(id, titleProcessed, contentsProcessed, articleLength);
    }
}
