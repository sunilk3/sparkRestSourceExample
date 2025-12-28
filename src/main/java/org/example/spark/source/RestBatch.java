package org.example.spark.source;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.example.spark.source.schema.Country;
import org.example.spark.source.client.RestCountriesClient;

import java.io.Serializable;
import java.util.*;

// The Batch defines how many partitions (parallel tasks) to run
class RestBatch implements Batch, Serializable {
    private final String url;
    private final Filter[] filters;
    private final StructType schema;
    //public RestBatch(String url, Filter[] filters, StructType schema)
    public RestBatch(String url, Filter[] filters, StructType schema)
    {
        this.url = url;
        this.schema = schema;
        this.filters = filters;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        // Strategy: Create 3 partitions to fetch 3 pages of data in parallel
        //return new InputPartition[]{ new RestPartition(1), new RestPartition(2), new RestPartition(3) };
        return new InputPartition[]{ new RestPartition() };
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RestReaderFactory(url, filters, schema);
    }
}

class RestPartition implements InputPartition {
    //private  final Filter[] filters;
    //public RestPartition(Filter[] filters) { this.filters = filters; }
    //public Filter[] getFilters() { return this.filters ; }
    public RestPartition() { }
}

class RestReaderFactory implements PartitionReaderFactory {
    private final String url;
    private final Filter[] filters;
    private final StructType schema;
    public RestReaderFactory(String url, Filter[] filters, StructType schema) {
        this.url = url;
        this.schema = schema;
        this.filters = filters;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new RestPartitionReader(url, filters, schema);
    }
}

class RestPartitionReader implements PartitionReader<InternalRow> {
    private final String baseUrl;
    private final StructType schema;
    private final Map<String,Object> queryParams = new HashMap<>();
    private final List<InternalRow> data = new ArrayList<>();
    private int index = -1;
    private String path = "/all"; //Default Path

    public RestPartitionReader(String baseUrl
            , Filter[] filters
            , StructType schema) {
        this.baseUrl = baseUrl;
        this.schema = schema;

        String[] validPaths = {"name", "capital", "region", "subregion"};
        // Translate Spark filters to api path
        for (Filter filter : filters) {
            if (filter instanceof EqualTo) {
                EqualTo eq = (EqualTo) filter;
                if(Arrays.asList(validPaths).contains(eq.attribute())){
                    path = String.format("/%s/%s",eq.attribute(),eq.value());
                }
            }
            //queryParams.putIfAbsent(eq.attribute(),eq.value());
        }

        // Construct URL with fields parameter: ?fields=id,title
        String fieldNames = String.join(",", schema.fieldNames());
        queryParams.putIfAbsent("fields",fieldNames);

        fetchData();
    }

    private void fetchData() {
        // Actual HTTP logic here...
        var restClient = new RestCountriesClient(baseUrl);
        var countries = restClient.Get(path, queryParams);
        for (Country country: countries) {
            var row = getRow(country);
            data.add(row);
        }
    }

    private GenericInternalRow getRow(Country country) {
        var noOfFields = schema.fields().length;
        Object[] values = new Object[noOfFields];
        for (int i = 0; i < noOfFields; i++) {
            String colName = schema.fieldNames()[i];
            switch (colName) {
                case "name":
                    values[i] = UTF8String.fromString(country.name);
                    break;
                case "capital":
                    values[i] = UTF8String.fromString(country.capital);
                    break;
                case "region":
                    values[i] = UTF8String.fromString(country.region);
                    break;
                case "subregion":
                    values[i] = UTF8String.fromString(country.subregion);
                    break;
                case "area":
                    values[i] = country.area;
                    break;
                case "population":
                    values[i] = country.population;
                    break;
                default:
                    values[i] = UTF8String.fromString("");
                    break;
            }
        }

        return new GenericInternalRow(values);
    }

    @Override
    public boolean next() {
        index++;
        return index < data.size();
    }

    @Override
    public InternalRow get() {
        // IMPORTANT: The InternalRow must strictly follow the pruned schema order
        Object[] values = new Object[schema.fields().length];
        for (int i = 0; i < schema.fields().length; i++) {
            var field = schema.fields()[i];
            var type = field.dataType();
            if(type.equals(DataTypes.DoubleType)){
                values[i] = data.get(index).getDouble(i);
            }
            else if(type.equals(DataTypes.LongType)){
                values[i] = data.get(index).getLong(i);
            }
            else {
                values[i] = data.get(index).getUTF8String(i);
            }
        }
        return new GenericInternalRow(values);
    }

    @Override
    public void close() {}
}