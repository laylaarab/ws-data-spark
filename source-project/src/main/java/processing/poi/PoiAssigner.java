package processing.poi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;

/**
 * Provides methods that aid in assigning a Processing.POI to request data
 */
public class PoiAssigner {
    /**
     * Iterates through a list of Processing.POI to assign a Processing.POI to each request
     * @param poiData
     * @param requestData
     * @return
     */
    public static Dataset<Row> iteratePoiList(Dataset<Row> poiData, Dataset<Row> requestData) {
        // Adding columns to store Processing.POI and distance data
        requestData = requestData.withColumn("POIID", functions.lit("none"));
        requestData = requestData.withColumn("POI_distance", functions.lit(-1));
        // Making Poi data into a list to iterate through it
        List<Row> poiList = poiData.collectAsList();
        for (Row poi : poiList) {
            requestData = assignClosestPoi(poi, requestData);
        }
        return requestData;
    }

    /**
     * Assigns the input Processing.POI to the request data if the distance to the Processing.POI is less than the distance to the previously inputted Processing.POI
     * @param poi
     * @param requestData
     * @return
     */
    private static Dataset<Row> assignClosestPoi(Row poi, Dataset<Row> requestData) {
        // Calculate distance to the Processing.POI
        requestData = calculatePoiDistance(poi, requestData);
        // Assign the ID of the Processing.POI
        requestData = requestData.withColumn("POIID",
                // Swap the Processing.POI ID in the request if the distance to the  new Processing.POI is smaller
                functions.when(requestData.col(poi.getAs("POIID") + "_distance")
                                .$less(requestData.col("POI_distance"))
                                // Swap also if the POI_distance value is invalid (-1)
                                .or(requestData.col("POI_distance").$eq$eq$eq(-1)),
                        poi.getAs("POIID"))
                        .otherwise(requestData.col("POIID"))
        );
        // Store the distance to the assigned Processing.POI
        requestData = requestData.withColumn("POI_distance",
                // Swap the Processing.POI distance in the request if the distance to the new Processing.POI is smaller
                functions.when(requestData.col(poi.getAs("POIID") + "_distance")
                                .$less(requestData.col("POI_distance"))
                                // Swap also if the POI_distance value is invalid (-1)
                                .or(requestData.col("POI_distance").$eq$eq$eq(-1)),
                        requestData.col(poi.getAs("POIID") + "_distance"))
                        .otherwise(requestData.col("POI_distance"))
        );
        // Removing extraneous columns
        return requestData.drop(poi.getAs("POIID") + "_a", poi.getAs("POIID") + "_distance");
    }

    /**
     * Distance calculated with the use of the Haversine formula
     * with the help of the following:
     * https://gist.github.com/pavlov99/bd265be244f8a84e291e96c5656ceb5c
     *
     * @param poi
     * @param requestData
     */
    private static Dataset<Row> calculatePoiDistance(Row poi, Dataset<Row> requestData) {
        // Calculate first part of Haversine, a
        requestData = requestData.withColumn(poi.getAs("POIID") + "_a",
                functions.pow(functions.sin(functions.radians((requestData.col("Latitude")
                        .minus(poi.getAs("Latitude")).divide(2)))), 2)
                        .plus(functions.cos(functions.radians(requestData.col("Latitude")))
                                .multiply(Math.cos(Math.toRadians(Double.parseDouble(poi.getAs("Latitude")))))
                                .multiply(functions.pow(functions.sin(functions.radians((requestData.col("Latitude")
                                        .minus(poi.getAs("Latitude")).divide(2)))), 2)))
        );
        // Use a to calculate final distance
        return requestData.withColumn(poi.getAs("POIID") + "_distance",
                functions.atan2(functions.sqrt(requestData.col(poi.getAs("POIID") + "_a")),
                        (functions.sqrt(requestData.col(poi.getAs("POIID") + "_a").multiply(-1).plus(1))))
                        .multiply(2).multiply(6371)
        );
    }
}
