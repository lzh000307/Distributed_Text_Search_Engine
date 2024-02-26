package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.*;


/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments method in local mode, although this may be overriden by
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
		if (sparkMasterDef==null) sparkMasterDef = "local[8]"; // default is local mode with two executors

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
		System.out.println("Spark runtime: " + (endTime - startTime) + " ms ("+ (double)(endTime - startTime)/ 60000+"min)");
		//Spark runtime: 96398 ms (1.6066333333333334min)
		//Spark runtime: 68245 ms (1.1374166666666667min)

		// Close the spark session
		//keep spark open and wait for the user to press enter
//		Scanner scanner = new Scanner(System.in);
//		System.out.println("Press enter to close the spark session");
//		scanner.nextLine();


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
//		CollectionAccumulator<String> queryTermsAccumulator = spark.sparkContext().collectionAccumulator();
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
//		queries.show();
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		// Spark context setup for accumulators and broadcasting
		LongAccumulator totalArticlesAccumulator = spark.sparkContext().longAccumulator("Total Articles");
		LongAccumulator totalLengthAccumulator = spark.sparkContext().longAccumulator("Total Article Length");
		QueryTermFrequencyAccumulator queryTermFrequencyAccumulator = new QueryTermFrequencyAccumulator();
		spark.sparkContext().register(queryTermFrequencyAccumulator, "Query Term Frequency");

		// Collect all unique query terms, avoid calculation(sum) errors
		List<String> allQueryTerms = queries.flatMap(
				(FlatMapFunction<Query, String>) query -> query.getQueryTerms().iterator(),
				Encoders.STRING()
		).collectAsList();
		// convert List<String> to hash set to remove duplicate
		Set<String> allQueryTermsSet = new HashSet<>(allQueryTerms);
	    // convert hash set to array list
		allQueryTerms = new ArrayList<>(allQueryTermsSet);
		List<Query> queryList = queries.collectAsList();

		// Create a list of queries, This entity is used to store the queries use a hash map.
		MyQueries myQueries = new MyQueries(queryList);

		// Broadcast variables for use in distributed computations
		final Broadcast<List<String>> broadcastedQueryTerms = spark.sparkContext().
				broadcast(allQueryTerms, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		final Broadcast<List<Query>> broadcastedQueryList = spark.sparkContext().
				broadcast(queryList, scala.reflect.ClassTag$.MODULE$.apply(List.class));

		// Process news articles, filtering and accumulating statistics
		Dataset<NewsArticleProcessed> newsArticleProcessedFiltered = news.flatMap(
				new NewsArticleFlatMap(broadcastedQueryTerms,
						totalArticlesAccumulator,
						totalLengthAccumulator,
						queryTermFrequencyAccumulator),
				Encoders.bean(NewsArticleProcessed.class));

		// Action to trigger transformations and cache the result because we need the value of the accumulators
		newsArticleProcessedFiltered.count();
		newsArticleProcessedFiltered.cache();

		/**
		 * Accumulator values are used after an action is triggered.
		 *
		 * According to the instruction, we feel that the blank paragraphs are also counted in the overall
		 * "first five paragraphs". Therefore, we have only skipped paragraphs with a value of null.
		 * This is slightly DIFFERENT from the results of skipping blank paragraphs, but it doesn't affect the result.
		 * If it does affect the result in some cases, please go to the NewsArticleFlatMap.java file
		 * to modify it (we commented the relevant code).
		 */
		System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
		System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());
		System.out.println("Total Query Term Frequency: " + queryTermFrequencyAccumulator.value());

		/**
		 * Shallow copy of the value in queryTermFrequencyAccumulator.
		 * Since String and Long are final types, shallow copying can ensure
		 * that the values inside queryTermFrequency are not affected by other data structures.
		 */
		Map<String, Long> queryTermFrequency = new HashMap<>(queryTermFrequencyAccumulator.value());

		// Broadcasting variables for DPH score calculation
		final Broadcast<Long> broadcastedTotalArticles = spark.sparkContext().
				broadcast(totalArticlesAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
		final Broadcast<Long> broadcastedTotalLength = spark.sparkContext().
				broadcast(totalLengthAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
		final Broadcast<Map<String, Long>> broadcastedQueryTermFrequencyMap = spark.sparkContext().
				broadcast(queryTermFrequency, scala.reflect.ClassTag$.MODULE$.apply(Map.class));

		// Compute DPH scores for each article-query pair
		Dataset<ResultWithQuery> resultWithQueryDataset = newsArticleProcessedFiltered.flatMap(
				new DPHScoreMap(broadcastedTotalArticles,
								broadcastedTotalLength,
								broadcastedQueryTermFrequencyMap,
								broadcastedQueryList),
				Encoders.bean(ResultWithQuery.class));
		// Collect results and rank documents
		List<ResultWithQuery> resultWithQueryList = resultWithQueryDataset.collectAsList();
		return RankingFunction.rankDocuments(resultWithQueryList, myQueries);
	}
}
