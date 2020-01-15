import processing.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import processing.poi.PoiAssigner;
import processing.poi.PoiCircleMaker;
import processing.poi.PoiStatisticCalculator;
import processing.poi.PoiVisualizer;
import reading.DataEntity;
import reading.DataInputHandler;

/**
 * Main entry point of application
 */
public class SparkAppMain {
    /**
     * All required cslculations shown in here
     *
     * @param args
     */
    public static void main(String[] args) {
        // Removing Spark log outputs (to make clear my own print statements)
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Creating entity to store request data
        DataEntity requestData = new DataEntity();
        DataEntity poiData = new DataEntity();

        // Reading csv file into Spark
        System.out.println("Now reading request csv data using Spark...");
        requestData.setEntityDataset(DataInputHandler.readCsvData("../data/DataSample.csv"));
        requestData.getEntityDataset().show();

        // Removing suspicious records from request dataset
        System.out.println("Now cleaning suspicious records from request dataset...");
        Dataset<Row> cleanedRequestData = DataCleaner.removeExtraneousRecords(requestData.getEntityDataset(),
                new String[]{"TimeSt", "Latitude", "Longitude"});
        // Show change in data cleaned
        cleanedRequestData.show();
        printChangeInDataRecordsAfterCleaning(requestData.getEntityDataset(), cleanedRequestData);
        // Setting our request data to the new dataset
        requestData.setEntityDataset(cleanedRequestData);

        // Inserting the Processing.POI data
        System.out.println("Inserting Processing.POI data into Spark...");
        poiData.setEntityDataset(DataInputHandler.readCsvData("../data/POIList.csv"));
        poiData.getEntityDataset().show();

        // Removing identical Processing.POI from the data
        System.out.println("Removing duplicates from Processing.POI data...");
        Dataset<Row> cleanedPoiData = DataCleaner.removeExtraneousRecords(poiData.getEntityDataset(),
                new String[]{"Latitude", "Longitude"});
        // Show change in data cleaned
        cleanedPoiData.show();
        printChangeInDataRecordsAfterCleaning(poiData.getEntityDataset(), cleanedPoiData);
        // Setting our request data to the new dataset
        poiData.setEntityDataset(cleanedPoiData);

        // Finding the nearest Processing.POI of each request by calculating minimum distance to each
        System.out.println("Assigning each request with its nearest Processing.POI...");
        requestData.setEntityDataset(PoiAssigner.iteratePoiList(poiData.getEntityDataset(),
                requestData.getEntityDataset()));
        requestData.getEntityDataset().show();

        // Calculating population variables
        System.out.println("Now calculating average and standard deviation of Processing.POI distance data");
        poiData.setEntityDataset(PoiStatisticCalculator.calculateAverageAndStddevForPoi(poiData.getEntityDataset(),
                requestData.getEntityDataset()));
        poiData.getEntityDataset().show();

        // Drawing circle for Processing.POI
        System.out.println("Now drawing a circle at each Processing.POI for the assigned requests");
        poiData.setEntityDataset(PoiCircleMaker.calculatePoiRequestDensity(poiData.getEntityDataset(),
                requestData.getEntityDataset()));
        poiData.getEntityDataset().show();

        // Plotting each Processing.POI on a scale of -10 to 10
        System.out.println("Now plotting our Processing.POI data...");
        poiData.setEntityDataset(new PoiVisualizer().plotPoiDataPopularity(poiData.getEntityDataset()));
        poiData.getEntityDataset().show();

    }

    /**
     * Helper to show change in data on update
     *
     * @param dataBefore
     * @param dataAfter
     */
    private static void printChangeInDataRecordsAfterCleaning(Dataset<Row> dataBefore, Dataset<Row> dataAfter) {
        // Calculating number of records removed by cleaning data
        long numRecordsBeforeCleaning = dataBefore.count();
        System.out.println("Number of rows in Request Data before removing suspicious records: "
                + numRecordsBeforeCleaning);
        long numRecordsAfterCleaning = dataAfter.count();
        System.out.println("Number of rows in Request Data after removing suspicious records: "
                + numRecordsAfterCleaning);
        long numRecordsRemovedByCleaning = numRecordsBeforeCleaning - numRecordsAfterCleaning;
        System.out.println("Number of records removed: " + numRecordsRemovedByCleaning + "\n");

    }

}
