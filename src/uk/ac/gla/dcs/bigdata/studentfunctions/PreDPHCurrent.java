package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.PreDPHCurrentData;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

public class PreDPHCurrent implements MapFunction<NewsArticleProcessed,PreDPHCurrentData>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3154454488137092785L;
	String targetQuery;
	public PreDPHCurrent(String targetQuery) {
		this.targetQuery = targetQuery;
	}
    public static int countOccurrences(String text, String sub) {
        int count = 0;
        int fromIndex = 0;
        
        // 当sub长度为0时，直接返回0避免死循环
        if(sub.length() == 0) return 0;

        while ((fromIndex = text.indexOf(sub, fromIndex)) != -1) {
            // 每次找到子字符串后，移动起始搜索位置
            fromIndex++;
            
            // 每找到一次，计数器加1
            count++;
        }

        return count;
    }
    
	@Override
	public PreDPHCurrentData call(NewsArticleProcessed value) throws Exception {
		List<String> contents = value.getContents();
		int termFrequencyInCurrentDocument = 0;
		int currentDocumentLength = 0;
		for (int i = 0; i<contents.size();i++) {
			String content = contents.get(i);
			termFrequencyInCurrentDocument = termFrequencyInCurrentDocument + countOccurrences(content,targetQuery);
			currentDocumentLength = currentDocumentLength + content.length();
		}

		PreDPHCurrentData currentData = new PreDPHCurrentData();
		currentData.setTermFrequencyInCurrentDocument(termFrequencyInCurrentDocument);
		currentData.setCurrentDocumentLength(currentDocumentLength);
		return currentData;
	}

}
