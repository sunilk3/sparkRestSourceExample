# Spark Rest Source Example

This example project demonstrates how to implement a Spark DataSource V2 that reads data from a REST API.

The sample implementation in this repository uses the public Rest Countries API and exposes a provider `RestTableProvider` that Spark can use via `DataFrameReader` or SQL. It supports:
- Automatic schema inference (see `RestTableProvider`)
- Batch reads via a `Table`/`Scan`/`Batch` implementation

## Build

Build with Maven:

```bash
mvn -DskipTests clean package
```

## Run the included example

The `Main` class contains a runnable example that creates a `SparkSession`, reads the REST source and runs a sample query.

Run with Maven:

```bash
mvn -q exec:java -Dexec.mainClass=org.example.spark.Main
```

Or build the JAR and run with `java -cp`:

```bash
java -cp target/sparkRestSourceExample-*.jar org.example.spark.Main
```

## Using the REST DataSource from code

Example Java usage (also shown in `Main`):

```java
SparkSession spark = SparkSession.builder()
	.appName("RestSourceExample")
	.master("local[*]")
	.getOrCreate();

You can register the provider as a temporary view and run SQL:

```java
spark.sql("CREATE TEMPORARY VIEW countries USING org.example.spark.source.RestTableProvider OPTIONS (url 'https://restcountries.com/v3.1')");
Dataset<Row> asian = spark.sql("SELECT name, capital, region FROM countries WHERE region = 'Asia'");
asian.show();
```

## Supported options and behavior

- `url` (required): Base URL for the REST API (e.g. `https://restcountries.com/v3.1`). The provider reads this option via `properties.get("url")`.
- Schema: `RestTableProvider#inferSchema` defines the schema: `name`, `capital`, `region`, `subregion`, `area`, `population`.
- Filter push-down: the source translates `EqualTo` filters on `name`, `capital`, and `region` into API paths (for example `/name/{value}`) when possible.

- Column pruning: the reader requests only the fields Spark needs via a `fields` query parameter.

The concrete translation and fetch logic are implemented in `RestScan`, `RestBatch`, and `RestPartitionReader`, with HTTP calls performed by `RestCountriesClient`.

## Project structure (key classes)

- `src/main/java/org/example/spark/source/RestTableProvider.java` — Implements `TableProvider` and supplies the schema and `RestTable` instance.
- `src/main/java/org/example/spark/source/RestTable.java` — Implements `SupportsRead` and returns a `ScanBuilder`.
- `src/main/java/org/example/spark/source/RestScanBuilder.java` — Handles filter push-down and column pruning, builds a `RestScan`.

- `src/main/java/org/example/spark/source/RestScan.java` — Creates a `RestBatch` for batch reads.
- `src/main/java/org/example/spark/source/RestBatch.java` — Plans partitions and creates readers that fetch data.
- `src/main/java/org/example/spark/source/client/RestCountriesClient.java` — HTTP client using Apache HttpClient and Jackson to map responses.

- `src/main/java/org/example/spark/Main.java` — Example runnable showing how to use the source from code.

## Notes & extension ideas

- The implementation is a minimal example: production sources should add retry/backoff, configurable timeouts, authentication support, and pagination strategies.
- To support Spark SQL `USING <format>` across clusters, ensure the JAR is available on the classpath of the Spark driver and executors (via `--jars` or ship the JAR).
- You can extend the client to support other endpoints, POST requests, or streaming if the REST API provides streaming endpoints.

## Troubleshooting

- If JSON mapping fails, check the external API shape or update the mapping in `RestCountriesClient`.
- If filters are not being pushed, verify the predicates are simple `=` filters on supported columns (name/capital/region).

---
