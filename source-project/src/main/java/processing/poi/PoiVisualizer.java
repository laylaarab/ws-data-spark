package processing.poi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Plots Processing.POI data on graph
 */
public class PoiVisualizer {
    private double averagePopularity;
    private double maxPopularity;
    private double minPopularity;

    public PoiVisualizer() {
    }

    /**
     * Uses the helpers below to plot the Processing.POI data
     * @param poiData
     * @return
     */
    public Dataset<Row> plotPoiDataPopularity(Dataset<Row> poiData) {
        calculateMaxPopularity(poiData);
        calculateMinPopularity(poiData);
        calculateAveragePopularity(poiData);
        return plotPoiPopularity(poiData);
    }

    private void calculateAveragePopularity(Dataset<Row> poiData) {
        double sumRequests = (double) poiData.agg(functions.sum("request_count").cast("double")).first().get(0);
        double sumArea = (double) poiData.agg(functions.sum("POI_area").cast("double")).first().get(0);
        averagePopularity = sumRequests / sumArea;
    }

    private Dataset<Row> plotPoiPopularity(Dataset<Row> poiData) {
        double max_upper_range = maxPopularity - averagePopularity;
        double max_lower_range = minPopularity - averagePopularity;
        System.out.println("average pop: " + averagePopularity);
        // Scale per unit
        double higher_scale = max_upper_range / 10;
        double lower_scale = max_lower_range / 10;
        // Plot each Processing.POI based on its request density
        return poiData.withColumn("plot",
                functions.when(poiData.col("request_density").$less$eq(averagePopularity),
                        poiData.col("request_density").divide(lower_scale))
                        .otherwise((poiData.col("request_density")).divide(higher_scale))
        );
    }

    private void calculateMaxPopularity(Dataset<Row> poiData) {
        double minRadius = (double) poiData.agg(functions.min("avg_distance").cast("double")).first().get(0);
        double maxRequests = (double) poiData.agg(functions.max("request_count").cast("double")).first().get(0);
        double minArea = Math.pow(minRadius, 2) * Math.PI;
        maxPopularity = maxRequests / minArea;
    }

    private void calculateMinPopularity(Dataset<Row> poiData) {
        double maxRadius = (double) poiData.agg(functions.max("POI_radius").cast("double")).first().get(0);
        double minRequests = (double) poiData.agg(functions.min("request_count").cast("double")).first().get(0);
        double maxArea = Math.pow(maxRadius, 2) * Math.PI;
        minPopularity = minRequests / maxArea;
    }

}
