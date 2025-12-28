package org.example.spark.source;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

public class RestTable implements SupportsRead {
    private final StructType schema;
    private final String url;

    public RestTable(StructType schema, String url) {
        this.schema = schema;
        this.url = url;
    }

    @Override
    public String name() { return "RestTable"; }

    @Override
    public StructType schema() { return schema; }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> caps = new HashSet<>();
        caps.add(TableCapability.BATCH_READ);
        return caps;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RestScanBuilder(url, schema);
    }
}