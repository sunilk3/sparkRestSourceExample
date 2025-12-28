package org.example.spark.source;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class RestScan implements Scan {
    private final String url;
    private final Filter[] filters;
    private final StructType schema;

    public RestScan(String url, Filter[] filters, StructType schema) {
        this.url = url;
        this.filters = filters;
        this.schema = schema;
    }

    @Override
    public StructType readSchema() {
        return schema; // Return only the columns Spark asked for
    }

    @Override
    public Batch toBatch() {
        // Pass the filters to the Batch so it can build the final URL
        return new RestBatch(url, filters, schema);
    }
    // ... schema() implementation
}