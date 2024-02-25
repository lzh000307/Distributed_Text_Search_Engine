
# Big Data Assessed Exercise Report
    
## Program Logic Summary
    
Our overall program starts with a comprehensive pre-processing stage, which includes cleaning the text data by removing stop words and applying stemming to reduce words to their base form. By setting up a Spark environment and reading query and news article data. It preprocesses this data into a structured format (queries and news articles) and sets up accumulators and broadcast variables for distributed processing. The main logic involves processing news articles based on queries, filtering and accumulating statistics, and then calculating DPH scores for article-query pairs. The results are aggregated, ranked, and finally written to disk. To optimize the task's efficiency amidst a vast dataset, our strategy minimizes dataset traversals to essential iterations only. Given the high cost of even a single pass through the data, we've deduced that at least two passes are indispensable. The initial pass employs accumulators to gather data critical for the subsequent DPH score computation. The second pass is then dedicated to this calculation. The concluding operation, sorting and identifying the top 10 distinct articles by DPH score, demands comparatively minor computational resources. In summary, this project represents a comprehensive solution for batch-based text search and filtering, combining the distributed processing capabilities of Apache Spark with advanced text processing technologies to provide a scalable, efficient, and effective document processing pipeline.
    
## Custom Functions
    
**DPHScoreMap:**  
It calculates the relevance score (DPH score) of each news article for a series of queries and outputs those results with scores above zero, alongside the queries they relate to. The function takes processed news articles (NewsArticleProcessed type) as input. Additionally, it uses Broadcast variables to receive the total number of articles, total length of all articles, a map of query term frequencies across the corpus, and a list of queries. For each input news article, the function calculates the DPH score for each query term. This score is based on the frequency of the query term in the article, its frequency across the entire corpus, the length of the article, the average length of documents in the corpus, and the total number of documents in the corpus. For each query, the function sums the DPH scores of its terms to get a total score for that query against the current article. If a query's total score for an article is greater than 0, the function outputs a ResultWithQuery object, which includes the score, article ID, article content, and query information. This function produces an iterator of a list of scoring results for each input article.
    
**NewsArticleFlatMap:**  
The function is to preprocess and analyse incoming news articles, calculating and recording information related to article length and query term frequencies, and outputting the results in a format that is convenient for further operations. It takes a NewsArticle object as input. Receives a list of query terms via a Broadcast variable, uses LongAccumulator to accumulate the total number of articles and their total length, and a custom accumulator QueryTermFrequencyAccumulator for accumulating query term frequencies. Preprocesses the article's title and content (up to the first five paragraphs), including tokenization, stop-word removal, etc. Counts the total number of words in the processed title and content to determine the article's length. Counts the frequency of each query term within the processed article. Adds the current article to the total count and length accumulators and updates the query term frequency accumulator. If the article's title is empty, contains only spaces, or if the article does not contain any of the query terms, it is excluded from further processing. For articles that meet the criteria, it outputs a NewsArticleProcessed object, which includes the article ID, article length, a map of query term frequencies, and the original NewsArticle object. These outputs are prepared for subsequent analysis or scoring calculations.
    
**RankingFunction**  
It converts search results into output by sorting, removing similar articles and outputting the top 10 most relevant data to meet the project requirements. It accepts a list of ResultWithQuery objects, each of which contains a query result and the corresponding query. A map is created to associate each query with its corresponding ranked results (a list of RankedResult). Iterates over the input list, and for each element, finds the corresponding query object based on the query and adds the query result to the list associated with that query in the map. If the map does not already contain a key for that query, a new list is created, and the result is added. Each query's result list is sorted in descending order based on scores. Then, deduplication is performed for the ranked results of each query, removing similar documents with a title textual distance less than 0.5, retaining only the highest-scoring document. Ensures that the result list for each query contains no more than 10 documents. Each processed query and its corresponding deduplicated document list are encapsulated into a DocumentRanking object, and these objects are collected as the final output returned.
    
## Efficiency Discussion
   
To do this task efficiently, we want to complete the computation with as few iterations of the dataset as possible. Because the dataset is extremely large, it is costly to iterate through it once. By analysing the task, we believe that we need to go through at least two iterations to complete the computation because the data from the accumulator of the first iteration is necessary for computing the DPH. The second time is to calculate the DPH score, and the last stage is not very computationally intensive because it only needs to sort and traverse 10 non-similar documents starting from the article with the high DPH score. 
   
We want to minimise double counting, e.g., different query may contain the same query term, then the DPH scores corresponding to these query terms can be counted only once. 
   
We broadcast all the QueryList, totalDocsInCorpus, totalDocumentLength, totalTermFrequencyInCorpusMap needed for DPH calculation so that the common parameters needed for these calculations are distributed to each executor.
   
We want to reduce the cache size. NewsArticleProcessed contains NewsArticle entity, which makes NewsArticleProcessed entity large, we have tried to exclude NewsArticle from NewsArticleProcessed, and keep only DocId and title to calculate the article similarity, and then find out the corresponding article when outputting the result. However, this implementation did not improve the efficiency of calculating DPH scores, but spent more time in finding articles, so we did not adopt this solution.
   
## Challenges
   
Encoders.bean is unable to properly serialise and reserialise data of type Map, which can cause some errors. We solved this by turning the Map data into a List containing two variables stored as entities, and then constructing it as a Map when used.
   
Two java variables of type int remain int after division, even when stored as double data types, which will cause averageDocumentLengthInCorpus to lose the value after the decimal point. Importantly, this will change the DPH scores of the articles, flipping the rankings of 2 articles with similar DPHs, and the results will likely retain the article that "actually" has the lower DPH score! This is a difficult problem to detect, but it does affect the output. It can be solved by simply converting the int type to a double type in the calculation. 
   
In order to make the program run more efficiently, we kept refactoring various functions. At the very

## Contributions

Everyone in this group has contributed an equal amount to this project.

Zhenghao LIN - 33.3%

Min MA - 33.3%

Meishan LIU - 33.3%