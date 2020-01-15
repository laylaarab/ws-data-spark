package processing.poi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Calculates various population statistics for Processing.POI
 */
public class PoiStatisticCalculator {
    /**
     * Wrapper to calculate both average and standard deviation for a Processing.POI
     *
     * @param poiData
     * @param requestData
     * @return
     */
    public static Dataset<Row> calculateAverageAndStddevForPoi(Dataset<Row> poiData, Dataset<Row> requestData) {
        poiData = poiData.join(calculateAverageDistance(requestData), "POIID");
        return poiData.join(calculateStandardDeviation(requestData), "POIID");

    }

    /**
     * Calculates average distance to Processing.POI based on request data
     *
     * @param requestData
     * @return
     */
    private static Dataset<Row> calculateAverageDistance(Dataset<Row> requestData) {
        return requestData.groupBy("POIID")
                .agg(functions.avg(requestData.col("POI_distance")).alias("avg_distance")
                );
    }

    /**
     * Calculates standard deviation of Processing.POI based on request data
     *
     * @param requestData
     * @return
     */
    private static Dataset<Row> calculateStandardDeviation(Dataset<Row> requestData) {
        return requestData.groupBy("POIID")
                .agg(functions.stddev(requestData.col("POI_distance")).alias("stddev_distance")
                );
    }


}
