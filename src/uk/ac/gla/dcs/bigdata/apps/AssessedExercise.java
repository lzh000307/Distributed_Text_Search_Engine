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
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		long startTime = System.currentTimeMillis();
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		long endTime = System.currentTimeMillis();
		System.out.println("Spark runtime: " + (endTime - startTime) + " ms ("+ (double)(endTime - startTime)/ 60000+"min)");
		//Spark runtime: 96398 ms (1.6066333333333334min)
		//Spark runtime: 68245 ms (1.1374166666666667min)

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
//		CollectionAccumulator<String> queryTermsAccumulator = spark.sparkContext().collectionAccumulator();
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		queries.show();
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
		// convert List<String> to hash set to remove duplicate
		Set<String> allQueryTermsSet = new HashSet<>(allQueryTerms);
	    // convert hash set to list
		allQueryTerms = new ArrayList<>(allQueryTermsSet);

		List<Query> queryList = queries.collectAsList();
		MyQueries myQueries = new MyQueries(queryList);


		final Broadcast<List<String>> broadcastedQueryTerms = spark.sparkContext().broadcast(allQueryTerms, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		final Broadcast<List<Query>> broadcastedQueryList = spark.sparkContext().broadcast(queryList, scala.reflect.ClassTag$.MODULE$.apply(List.class));
		/**
		 * Build the relationship between List<String> allQueryTerms and quires
		 */

		// Convert NewsArticle
		Dataset<NewsArticleProcessed> newsArticleProcessed = news.map(new NewsArticleMap(broadcastedQueryTerms, totalArticlesAccumulator, totalLengthAccumulator, queryTermFrequencyAccumulator), Encoders.bean(NewsArticleProcessed.class));

		// execute the map function
		// COUNT
		newsArticleProcessed.count();
//		System.out.println("Total Articles Processed: " + totalArticlesAccumulator.value());
//		System.out.println("Total Article Length Sum: " + totalLengthAccumulator.value());
//		System.out.println("Query Term Frequency: " + queryTermFrequencyAccumulator.value());
		// Compute Query frequency
		Map<String, Long> queryTermFrequency = queryTermFrequencyAccumulator.value();
		/**
		 * TODO: Convert to spark dataset
		 * temp solution: for loop
		 */
//		List<QueryFrequency> queryFrequencyList = new ArrayList<>();
		Map<Query, Integer> queryFrequencyMap = new HashMap<>();
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
				queryFrequencyMap.put(query, count);
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
//		List<NewsArticleProcessed> newsArticleProcessedList = newsArticleProcessedFiltered.collectAsList();

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
		// execute the map function
		resultWithQueryDataset.count();
		// convert to list
		List<ResultWithQuery> resultWithQueryList = resultWithQueryDataset.collectAsList();
		// convert to DocumentRanking
		Map<Query, List<RankedResult>> resultMap = new HashMap<>();
		for(ResultWithQuery resultWithQuery : resultWithQueryList) {
			Query query = myQueries.getQuery(resultWithQuery.getQuery());
			RankedResult rankedResult = resultWithQuery.getRankedResult();
			if(resultMap.containsKey(query)) {
				resultMap.get(query).add(rankedResult);
			} else {
				List<RankedResult> rankedResults = new ArrayList<>();
				rankedResults.add(rankedResult);
				resultMap.put(query, rankedResults);
			}
		}
		// convert to list
		List<DocumentRanking> documentRankings = new ArrayList<>();
		for(Map.Entry<Query, List<RankedResult>> entry : resultMap.entrySet()) {
			Query query = entry.getKey();
			List<RankedResult> rankedResults = entry.getValue();
			// sort the list
			Collections.sort(rankedResults);
			// reverse
			Collections.reverse(rankedResults);
			documentRankings.add(new DocumentRanking(query, rankedResults));
		}
		// now we have a List of DocumentRanking
		// compare similarity and find the top 10
		// new a output list
		List<DocumentRanking> output = new ArrayList<>();
		for(DocumentRanking ranking : documentRankings){
			int resultNum = 0;
			int loopLength = ranking.getResults().size();
			// for speed up
			List<RankedResult> rr = ranking.getResults();
			List<RankedResult> newRR = new ArrayList<>();
			for(int i = 0; i < loopLength - 1; i++){
				if(resultNum >= 10)
					break;
				// not enough 10 results
				// compare similarity
				/**
				 *
				 */
				String t1 = rr.get(i).getArticle().getTitle();
				String t2 = rr.get(i+1).getArticle().getTitle();
				newRR.add(rr.get(i));
				resultNum++;
				if(t1 != null && t2 != null) {
					double distance = TextDistanceCalculator.similarity(t1, t2);
					if (distance < 0.5) {
						i++; // skip the next one
					}
				}

			}
			if(rr.size() < 10){
				System.out.println("WARNING: Query " + ranking.getQuery().getOriginalQuery() + " has less than 10 results");
				//TODO: add more results
			}
			output.add(new DocumentRanking(ranking.getQuery(), newRR));
		}
		return output;







//		// 使用groupByKey对resultWithQueryDataset按Query进行分组
//		JavaPairRDD<Query, Iterable<RankedResult>> groupedResults = resultWithQueryDataset
//				.toJavaRDD() // 转换为JavaRDD
//				.mapToPair(resultWithQuery -> new Tuple2<>(resultWithQuery.getQuery(), resultWithQuery.getRankedResult())) // 将Dataset转换为键值对RDD
//				.groupByKey(); // 按键（Query）进行分组
//
//		// 将每个组转换为DocumentRanking对象
//		JavaRDD<DocumentRanking> documentRankingsRDD = groupedResults.map(
//				new org.apache.spark.api.java.function.Function<Tuple2<Query, Iterable<RankedResult>>, DocumentRanking>() {
//					public DocumentRanking call(Tuple2<Query, Iterable<RankedResult>> queryAndResults) throws Exception {
//						List<RankedResult> resultsList = new ArrayList<>();
//						queryAndResults._2().forEach(resultsList::add);
//						return new DocumentRanking(queryAndResults._1(), resultsList);
//					}
//				});
//		documentRankingsRDD.count();
//
////		// 将JavaRDD<DocumentRanking>转换为List<DocumentRanking>以返回
//		List<DocumentRanking> documentRankings = documentRankingsRDD.collect();
//		for(int i=0; i<10; i++) {
//			System.out.println("DocumentRanking: " + documentRankings.get(1).getQuery().getOriginalQuery() + " " + documentRankings.get(0).getResults().get(i).getScore());
//		}
//		//
//
//		SimilarityFilter filter = new SimilarityFilter();
//		List<DocumentRanking> filteredRankings = new ArrayList<>();
//		for (DocumentRanking ranking : documentRankings) {
//			try {
//				DocumentRanking filteredRanking = filter.call(ranking);
//				System.out.println("Query: " + ranking.getQuery().getOriginalQuery() + " - Initial Results: " + ranking.getResults().size());
//				List<RankedResult> topResults = filteredRanking.getResults().stream()
//						.sorted(Comparator.comparing(RankedResult::getScore).reversed()) // Sort by score in descending order
//						.limit(10) // Limit to top 10
//						.collect(Collectors.toList());
//
//				DocumentRanking finalRanking = new DocumentRanking(filteredRanking.getQuery(), topResults);
//				filteredRankings.add(filteredRanking);
//				System.out.println("Query: " + finalRanking.getQuery().getOriginalQuery() + " - Filtered Results: " + finalRanking.getResults().size());
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		// Print the filtered rankings
//		for (DocumentRanking ranking : filteredRankings) {
//			System.out.println("Query: " + ranking.getQuery().getOriginalQuery());
//			for (RankedResult result : ranking.getResults()) {
//				System.out.println("\t" + result.getArticle().getTitle() + " - Score: " + result.getScore());
//			}
//		}
//		return null;



//		// group by query
//		Dataset<Row> sortedDataset = resultWithQueryDataset
//				.groupBy("query") // 根据query分组
//				.agg(collect_list(struct(col("rankedResult"), col("rankedResult.score").alias("score"))).alias("results")) // 收集每个query的RankedResult列表
//				.withColumn("sortedResults", sort_array(col("results"), false)) // 对每个列表按score降序排列
//				.select(col("query"), col("sortedResults.rankedResult").alias("sortedRankedResults")); // 选择并重命名列
//		List<Row> sortedList = sortedDataset.collectAsList();
//		Map<String, List<RankedResult>> resultMap = new HashMap<>();
//
//		for (Row row : sortedList) {
//			String query = row.getString(0); // 获取query值
//			List<Row> rankedResultsRows = row.getList(1); // 获取对应的RankedResult列表
//
//			List<RankedResult> rankedResults = new ArrayList<>();
//			for (Row rankedResultRow : rankedResultsRows) {
//				String docid = rankedResultRow.getAs("docid");
//				double score = rankedResultRow.getAs("score");
//				NewsArticle article = rankedResultRow.getAs("article");
//				// 然后使用这些字段值创建RankedResult实例
//				RankedResult rankedResult = new RankedResult(docid, article, score); // 根据你的实际构造函数调整这里
//				rankedResults.add(rankedResult);
//			}
//
//			resultMap.put(query, rankedResults);
//			System.out.println(query + " " + rankedResults.size() + " " + rankedResults.get(0).getScore());
//		}




//		resultWithQueryDataset.show();
		// group by query
//		Dataset<Row> df = resultWithQueryDataset.toDF();
//		Dataset<Row> grouped = df.groupBy("query").agg(collect_list("rankedResult").alias("collectedRankedResults"));
//
//		Dataset<ResultList> finalResults = grouped.map(
//				(MapFunction<Row, ResultList>) row -> {
//					List<RankedResult> rankedResults = row.getList(row.fieldIndex("collectedRankedResults"));
//					System.out.println(row.getString(0));
//					ResultList aggregate = new ResultList(row.getString(0), row.getList(1));
//					return aggregate;
//				},
//				Encoders.bean(ResultList.class)
//		);
//		finalResults.count();
//		List<ResultList> fr = finalResults.collectAsList();
//		System.out.println(fr.get(0).getRankedResultList());


//		Dataset<Row> df = resultWithQueryDataset.toDF();
//		Dataset<Row> documentRankingDataset = resultWithQueryDataset.groupBy("query").agg(collect_list("rankedResult").alias("collectedRankedResults"));
//		//		Dataset<DocumentRanking> documentRankingDataset = resultWithQueryDataset.groupBy("query").agg(collect_list("rankedResult").as("rankedResults")).as(Encoders.bean(DocumentRanking.class));
//		//		documentRankingDataset.show();
//
//		// convert to dataset
//		Encoder<ResultList> encoder = Encoders.bean(ResultList.class);
//		Dataset<ResultList> aggregatedDataset = documentRankingDataset.map(row -> {
//			String query = row.getAs("query");
//			List<RankedResult> collectedRankedResults = row.getList(row.fieldIndex("collectedRankedResults"));
//			// 假设你有一个接收这些参数的构造函数或者一个方法来设置它们
//			return new ResultList(query, collectedRankedResults);
//		}, encoder);


//		return null; // replace this with the the list of DocumentRanking output by your topology
	}
}
