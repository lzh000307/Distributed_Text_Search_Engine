package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;


import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.codehaus.jackson.map.type.CollectionType;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.*;

import static org.apache.spark.sql.functions.*;


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
		if (sparkMasterDef==null) sparkMasterDef = "local[1]"; // default is local mode with two executors

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
		Scanner scanner = new Scanner(System.in);
		System.out.println("Press enter to close the spark session");
		scanner.nextLine();


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
		SparkContext sc = spark.sparkContext();

		LongAccumulator totalArticlesAccumulator = sc.longAccumulator("Total Articles");
		LongAccumulator totalLengthAccumulator = sc.longAccumulator("Total Article Length");

		QueryTermFrequencyAccumulator queryTermFrequencyAccumulator = new QueryTermFrequencyAccumulator();
		spark.sparkContext().register(queryTermFrequencyAccumulator, "Query Term Frequency");


		List<String> allQueryTerms = queries.flatMap(
				(FlatMapFunction<Query, String>) query -> query.getQueryTerms().iterator(),
				Encoders.STRING()
		).collectAsList();
		// convert List<String> to hash set to remove duplicate
		Set<String> allQueryTermsSet = new HashSet<>(allQueryTerms);
	    // convert hash set to list
		allQueryTerms = new ArrayList<>(allQueryTermsSet);

		List<Query> queryList = queries.collectAsList();
		MyQueries myQueries = new MyQueries(queryList);
		final Broadcast<List<String>> broadcastedQueryTerms = spark.sparkContext().broadcast(allQueryTerms, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		final Broadcast<List<Query>> broadcastedQueryList = spark.sparkContext().broadcast(queryList, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		// Convert NewsArticle
//		Dataset<NewsArticleProcessed> newsArticleProcessed = news.map(new NewsArticleMap(broadcastedQueryTerms, totalArticlesAccumulator, totalLengthAccumulator, queryTermFrequencyAccumulator), Encoders.bean(NewsArticleProcessed.class));
		Dataset<NewsArticleProcessed> newsArticleProcessedFiltered = news.flatMap(new NewsArticleFlatMap(broadcastedQueryTerms, totalArticlesAccumulator, totalLengthAccumulator, queryTermFrequencyAccumulator), Encoders.bean(NewsArticleProcessed.class));

		// COUNT
		newsArticleProcessedFiltered.count();
		newsArticleProcessedFiltered.persist(StorageLevel.MEMORY_AND_DISK()); // cache the dataset
		// execute the map function

//		System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
//		System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());
//		System.out.println("Query Term Frequency 0: " + queryTermFrequencyAccumulator.value());
		// Compute Query frequency
		Map<String, Long> queryTermFrequency = new HashMap<>();
		queryTermFrequency.putAll(queryTermFrequencyAccumulator.value());
		System.out.println("Query Term Frequency 0.1: " + queryTermFrequencyAccumulator.value());
		System.out.println("queryTermFrequency: " + queryTermFrequency);
		System.out.println("Query Term Frequency 0.9: " + queryTermFrequencyAccumulator.value());
		//convert to list
		//boardcast the totalArticlesAccumulator and totalLengthAccumulator
		final Broadcast<Long> broadcastedTotalArticles = spark.sparkContext().broadcast(totalArticlesAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
		final Broadcast<Long> broadcastedTotalLength = spark.sparkContext().broadcast(totalLengthAccumulator.value(), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
//		final Broadcast<Map> broadcastedQueryFrequencyMap = spark.sparkContext().broadcast(queryFrequencyMap, scala.reflect.ClassTag$.MODULE$.apply(Map.class));
		final Broadcast<Map> broadcastedQueryTermFrequencyMap = spark.sparkContext().broadcast(queryTermFrequency, scala.reflect.ClassTag$.MODULE$.apply(Map.class));

		//compute
		Dataset<ResultWithQuery> resultWithQueryDataset = newsArticleProcessedFiltered.flatMap(new DPHScoreMap(broadcastedTotalArticles, broadcastedTotalLength, broadcastedQueryTermFrequencyMap, broadcastedQueryList), Encoders.bean(ResultWithQuery.class));
		// execute the map function
		resultWithQueryDataset.count();
		// print queryTermFrequency accumulator
		// convert to list
		List<ResultWithQuery> resultWithQueryList = resultWithQueryDataset.collectAsList();
		return RankingFunction.rankDocuments(resultWithQueryList, myQueries);
	}
}
