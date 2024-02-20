package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.*;

import static org.apache.spark.sql.functions.collect_list;


/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {


	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		long startTime = System.currentTimeMillis();
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		long endTime = System.currentTimeMillis();
		System.out.println("Time: " + (endTime - startTime) + "ms");

		// Close the spark session
		spark.close();


		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}



	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		// Convert Query
//		List<String> queryTerms = new ArrayList<>();
//		Set<String> queryTermsSet = new HashSet<>();
//		for(Query query : queries.collectAsList()) {
//			queryTermsSet.addAll(query.getQueryTerms());
//		}
//			queryTerms.addAll(queryTermsSet);
//			System.out.println(queryTerms);
//		}



		SparkContext sc = spark.sparkContext();

		LongAccumulator totalArticlesAccumulator = sc.longAccumulator("Total Articles");
		LongAccumulator totalLengthAccumulator = sc.longAccumulator("Total Article Length");

		QueryTermFrequencyAccumulator queryTermFrequencyAccumulator = new QueryTermFrequencyAccumulator();
		spark.sparkContext().register(queryTermFrequencyAccumulator, "Query Term Frequency");


		List<String> allQueryTerms = queries.flatMap(
				(FlatMapFunction<Query, String>) query -> query.getQueryTerms().iterator(),
				Encoders.STRING()
		).collectAsList();

		// convert Dataset<Query> quires to HashMap<String, List<String>> allQueryTerms
		List<Query> queryList = queries.collectAsList();


		final Broadcast<List<String>> broadcastedQueryTerms = spark.sparkContext().broadcast(allQueryTerms, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		final Broadcast<List<Query>> broadcastedQueryList = spark.sparkContext().broadcast(queryList, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		/**
		 * Build the relationship between List<String> allQueryTerms and quires
		 */

		// Convert NewsArticle
		//122648418750842

		Dataset<NewsArticleProcessed> newsArticleProcessed = news.map(new NewsArticleMap(broadcastedQueryTerms, totalArticlesAccumulator, totalLengthAccumulator, queryTermFrequencyAccumulator), Encoders.bean(NewsArticleProcessed.class));

		// execute the map function
		// COUNT
		newsArticleProcessed.count();
		System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
		System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());
		System.out.println("Query Term Frequency: " + queryTermFrequencyAccumulator.value());
		// Compute Query frequency
		Map<String, Long> queryTermFrequency = queryTermFrequencyAccumulator.value();
		/**
		 * TODO: Convert to spark dataset
		 * temp solution: for loop
		 */
//		List<QueryFrequency> queryFrequencyList = new ArrayList<>();
		Map<String, Integer> queryFrequencyMap = new HashMap<>();
		for(Query query : queryList) {
			int count = 0;
			Long count1 = 0L;
			int numOfTerms = 0;
			int length = query.getQueryTerms().size();
			for(int i=0; i<length; i++) {
				count1= queryTermFrequency.getOrDefault(query.getQueryTerms().get(i), 0L);
				count += Long.valueOf(count1).intValue();
				numOfTerms += query.getQueryTermCounts()[i];
			}
			if(count != 0) {
//				QueryFrequency queryFrequency = new QueryFrequency(query.getOriginalQuery(), count/numOfTerms);
				queryFrequencyMap.put(query.getOriginalQuery(), count);
//				queryFrequencyMap.put(query.getOriginalQuery(), count/numOfTerms);
			}
		}
		// Convert to spark dataset
//		Dataset<QueryFrequency> queryFrequency = spark.createDataset(queryFrequencyList, Encoders.bean(QueryFrequency.class));



		/**
		 * Reduce newArticleProcessed, delete the articles that do not contain the query terms
		 */
		Dataset<NewsArticleProcessed> newsArticleProcessedFiltered = newsArticleProcessed.filter(newsArticleProcessed.col("hitQueryTerms").equalTo(true));
		// COUNT
		newsArticleProcessedFiltered.show();
		//convert to list
		List<NewsArticleProcessed> newsArticleProcessedList = newsArticleProcessedFiltered.collectAsList();

		/**
		 * Calculate each query term's frequency in each article
		 */
		// Build the relationship between List<String> allQueryTerms and quires
		Dataset<QueryWithArticle> queryWithArticle = newsArticleProcessedFiltered.flatMap(new QueryWithArticleMap(broadcastedQueryList), Encoders.bean(QueryWithArticle.class));
		// COUNT
//		queryWithArticle.count();
//		 queryWithArticle.count();

		//boardcast the totalArticlesAccumulator and totalLengthAccumulator
		final Broadcast<Long> broadcastedTotalArticles = spark.sparkContext().broadcast(totalArticlesAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
		final Broadcast<Long> broadcastedTotalLength = spark.sparkContext().broadcast(totalLengthAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
		final Broadcast<Map> broadcastedQueryFrequencyMap = spark.sparkContext().broadcast(queryFrequencyMap, scala.reflect.ClassTag$.MODULE$.apply(Map.class));
		//compute
		Dataset<ResultWithQuery> resultWithQueryDataset = queryWithArticle.flatMap(new DPHScoreMap(broadcastedTotalArticles, broadcastedTotalLength, broadcastedQueryFrequencyMap), Encoders.bean(ResultWithQuery.class));
		// COUNT



		resultWithQueryDataset.show();
		// group by query
		Dataset<Row> documentRankingDataset = resultWithQueryDataset.groupBy("query").agg(collect_list("rankedResult").alias("collectedRankedResults"));



//		Dataset<DocumentRanking> documentRankingDataset = resultWithQueryDataset.groupBy("query").agg(collect_list("rankedResult").as("rankedResults")).as(Encoders.bean(DocumentRanking.class));
		documentRankingDataset.show();

		return null; // replace this with the the list of DocumentRanking output by your topology
	}


}
