package org.example.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class EcommerceLogSchema {
    public static StructType getEcommerceLogSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("event_time", DataTypes.StringType, false),
                DataTypes.createStructField("event_type", DataTypes.StringType, false),
                DataTypes.createStructField("product_id", DataTypes.StringType, true),
                DataTypes.createStructField("category_id", DataTypes.StringType, true),
                DataTypes.createStructField("category_code", DataTypes.StringType, true),
                DataTypes.createStructField("brand", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, false),
                DataTypes.createStructField("user_session", DataTypes.StringType, false)
        });
    }
}
