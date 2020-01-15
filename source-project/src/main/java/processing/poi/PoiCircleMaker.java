package processing.poi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Creates circles of influence around Processing.POI and data based on the circle
 */
public class PoiCircleMaker {
    /**
     * Counts the number of requests received by a Processing.POI, helper for Processing.POI density
     * @param requestData
     * @return
     */
    private static Dataset<Row> poiRequestCount(Dataset<Row> requestData) {
        return requestData.groupBy("POIID")
                .agg(functions.count("POIID").alias("request_count"));
    }

    /**
     * Calculates the radius of a Processing.POI based on the furthest request assigned to a Processing.POI, helper for Processing.POI density
     * @param requestData
     * @return
     */
    private static Dataset<Row> calculatePoiRadius(Dataset<Row> requestData) {
        return requestData.groupBy("POIID")
                .agg(functions.max("POI_distance").alias("POI_radius"));
    }

    /**
     * Calculates the area of the circle of influence of the Processing.POI using the formula PI*r^2, helper for Processing.POI density
     * @param poiData
     * @return
     */
    private static Dataset<Row> calculatePoiArea(Dataset<Row> poiData) {
        return poiData.withColumn("POI_area",
                functions.pow(poiData.col("POI_radius"), 2).multiply(Math.PI));
    }

    /**
     * Calculates the requests/area of the Processing.POI circle of influence
     * @param poiData
     * @param requestData
     * @return
     */
    public static Dataset<Row> calculatePoiRequestDensity(Dataset<Row> poiData, Dataset<Row> requestData) {
        poiData = poiData.join(calculatePoiRadius(requestData), "POIID");
        poiData = calculatePoiArea(poiData);
        poiData = poiData.join(poiRequestCount(requestData), "POIID");
        return poiData.withColumn("request_density",
                poiData.col("request_count").divide(poiData.col("POI_area")));
    }

}


