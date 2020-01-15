package controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Builds a Spark session to be used everywhere in the application
 */
public class SparkSessionBuilder {
    /**
     * The current Spark session
     */
    private static SparkSession spark;

    /**
     * Instance-controlled instantiation of the SparkSession
     *
     * @return Singleton instance of the SparkSession
     */
    public static SparkSession getSparkSession() {
        if (spark == null) {
            SparkSessionBuilder.initSparkSession();
        }
        return SparkSessionBuilder.spark;
    }

    /**
     * Helper that initiates Spark session when getSparkSession() called for the first time
     */
    private static void initSparkSession() {
        // Set up the spark config
        SparkConf sparkConf = new SparkConf()
                .setAppName("EQ-sample")
                .setMaster("local[*]");
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
    }
}
