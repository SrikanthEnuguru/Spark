package sparkTransformations;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BasicTransformations {

	public static void main(String[] args) {
		
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 3));		
		inputRDD.collect().forEach(x -> System.out.println(x));
		
		//map example
		JavaRDD<Integer> mapRDDs=inputRDD.map(x -> x*x);
		mapRDDs.collect().forEach(x -> System.out.println(x));		
		
		//filter example
		JavaRDD<Integer> filteredRDDs=inputRDD.filter(x -> x!=3);
		System.out.println(StringUtils.join(filteredRDDs.collect(), ","));
		//filteredRDDs.collect().forEach(x -> System.out.println(x));
		
		//flatMap example
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
		JavaRDD<String> words = lines.flatMap(str -> Arrays.asList(str.split(" ")).iterator());
		System.out.println(StringUtils.join(words.collect(), ","));
		
		
		
		
		sc.stop();
	}

}
