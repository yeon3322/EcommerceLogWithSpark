package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.example.schema.EcommerceLogSchema;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("[Usage: spark-submit --class org.example.Main " +
                    "--master [master] [jar_file] [file_name]]");
            return;
        }

        String localDataPath = "file://" + System.getProperty("user.dir") + "/data/" + args[0];

        String hdfsCommonPath = "hdfs://localhost:9000";
        String outputPath = hdfsCommonPath + "/output/parquet";
        String checkPointPath = hdfsCommonPath + "/checkpoints";
        String markerPath = hdfsCommonPath + "/markers/" + args[0];

        SparkSession spark = SparkSession.builder()
                .appName("eCommerce behavior Log ETL with Spark")
                .config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.warehouse.dir", "/user/hive/warehouse/")
                .enableHiveSupport()
                .master("local")
                .getOrCreate();

        spark.sparkContext().setCheckpointDir(checkPointPath);

        try {
            Configuration hdfsConf = spark.sparkContext().hadoopConfiguration();
            hdfsConf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem hdfs = FileSystem.get(hdfsConf);
            Path markerFilePath = new Path(markerPath);

            if (hdfs.exists(markerFilePath)) {
                System.out.println("Already processed. Skip process.");
                return;
            }

            Dataset<Row> data = spark.read()
                    .option("header", "true")
                    .schema(EcommerceLogSchema.getEcommerceLogSchema())
                    .csv(localDataPath);

            data = data.withColumn("event_date", functions.to_date(
                    functions.from_utc_timestamp(data.col("event_time"), "Asia/Seoul"), "yyyy-MM-dd"));

            data.checkpoint();

            data.write()
                    .mode("overwrite")
                    .partitionBy("event_date")
                    .parquet(outputPath);

            spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_log (" +
                    "event_time STRING, event_type STRING, product_id STRING, category_id STRING, " +
                    "category_code STRING, brand STRING, price DOUBLE, user_id STRING, user_session STRING) " +
                    "PARTITIONED BY (event_date DATE) STORED AS PARQUET LOCATION '" + outputPath + "'");

            // 중복 작업을 피하기 위한 marker 생성
            hdfs.create(markerFilePath).close();
        } catch(Exception e) {
            System.err.println("Processing failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}