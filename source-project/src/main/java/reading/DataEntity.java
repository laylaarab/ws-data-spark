package reading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Entity that stores a Dataset member variable
 */
public class DataEntity {
    /**
     * Entity data wrapped in by class
     */
    private Dataset<Row> entityData;

    public Dataset<Row> getEntityDataset() {
        return entityData;
    }

    public void setEntityDataset(Dataset<Row> entityData) {
        this.entityData = entityData;
    }
}
