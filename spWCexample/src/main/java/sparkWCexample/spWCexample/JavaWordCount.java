package sparkWCexample.spWCexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaWordCount {

	public static void main(String[] args) {
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile("C:\\Users\\1021935\\Spark-Workspace\\spWCexample\\log.txt");
		
		JavaRDD<String> errorsRDD = input.filter(x -> x.contains("ERROR"));
		JavaRDD<String> warningsRDD = input.filter(x -> x.contains("WARN"));
		
		System.out.println("No of Errors: "+errorsRDD.count());
		System.out.println("No of Warnings: "+warningsRDD.count());
		sc.close();

	}

}
