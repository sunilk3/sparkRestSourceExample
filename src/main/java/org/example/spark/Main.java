package org.example.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        var url = "https://restcountries.com/v3.1";
        SparkSession spark = SparkSession.builder()
                .appName("Spark Read Example")
                .master("local")
                .config("spark.ui.enabled", "false") // Disables the Web UI
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("org.example.spark.source.RestTableProvider")
                .option("url", url)
                .load();

        //df.show(400);
        df.createOrReplaceTempView("country");
        df.printSchema();

        Dataset<Row> resultDF = spark
                //.sql("SELECT name, capital, region FROM country WHERE region = 'Asia'");
                .sql("SELECT name, capital, region, subregion, area, population FROM country WHERE capital = 'New Delhi'");

        // 5. Show results
        resultDF.show(80);

        spark.stop();
    }
}