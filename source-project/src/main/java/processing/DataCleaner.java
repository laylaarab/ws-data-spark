package processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Wraps methods that allow in the removal of suspicious and extraneous records
 */
public class DataCleaner {
    /**
     * Cleans data by removing extraneous records based on input columns
     * @param requestData
     * @param cols
     * @return
     */
    public static Dataset<Row> removeExtraneousRecords(Dataset<Row> requestData, String[] cols) {
        return requestData.dropDuplicates(cols);
    }

}
