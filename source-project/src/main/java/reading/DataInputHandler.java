package reading;

import controller.SparkSessionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class that aids in data input into Spark
 */
public class DataInputHandler {
    /**
     * Inputs csv data to Spark
     *
     * @param csvPath
     * @return
     */
    public static Dataset<Row> readCsvData(String csvPath) {
        SparkSession spark = SparkSessionBuilder.getSparkSession();
        return spark.read()
                .option("header", "true")    // First row has headers
                .csv(csvPath);
    }

    /**
     * Aids in the input of relation files for pipeline dependenncy calculations
     *
     * @param txtPath
     */
    public static Dataset<String> readRelations(String txtPath) {
        SparkSession spark = SparkSessionBuilder.getSparkSession();
        return spark.read()
                .option("delimiter", "->")
                .textFile(txtPath);
    }

    /**
     * Aids in the input of task files for pipeline dependenncy calculations
     *
     * @param csvPath
     * @return
     */
    public static Dataset<Row> readTasks(String csvPath) {
        SparkSession spark = SparkSessionBuilder.getSparkSession();
        return spark.read()
                .option("header", "false")
                .csv(csvPath);
    }
}