package org.example.spark.source;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RestScanBuilder implements SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    private final String baseUrl;
    private Filter[] pushedFilters = new Filter[0];
    private StructType prunedSchema;

    public RestScanBuilder(String url, StructType fullSchema) {
        this.baseUrl = url;
        this.prunedSchema = fullSchema; // Default to all columns
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> supported = new ArrayList<>();
        List<Filter> unsupported = new ArrayList<>();

        for (Filter filter : filters) {
            // Check if we can translate this filter to a URL parameter
            if (filter instanceof EqualTo &&
                    (
                            ((EqualTo) filter).attribute().equals("name") ||
                            ((EqualTo) filter).attribute().equals("capital") ||
                            ((EqualTo) filter).attribute().equals("region")
                    )
            ) {
                supported.add(filter);
            } else {
                unsupported.add(filter);
            }
        }

        this.pushedFilters = supported.toArray(new Filter[0]);
        // Return filters Spark still needs to evaluate itself (unsupported ones)
        return unsupported.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        // Spark tells us exactly which columns are needed
        this.prunedSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        return new RestScan(baseUrl, pushedFilters, prunedSchema);
    }
}