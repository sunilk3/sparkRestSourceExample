package org.example.spark.source;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class RestTableProvider implements TableProvider {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {

        // Define the schema expected from the API
        return new StructType()
                .add("name", DataTypes.StringType)
                .add("capital", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("subregion", DataTypes.StringType)
                .add("area", DataTypes.DoubleType)
                .add("population", DataTypes.LongType)
                ;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new RestTable(schema, properties.get("url"));
    }
}