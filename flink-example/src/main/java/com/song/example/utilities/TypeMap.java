package com.song.example.utilities;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

public class TypeMap {

    public static final Map<String, LogicalType> AVRO = new HashMap<String, LogicalType>();
    public static final Map<String, LogicalType> AVRO_NULLABLE = new HashMap<String, LogicalType>();
    public static final Map<String, String> HUDI = new HashMap<String, String>();

    private static final String STRING = "STRING";
    private static final String BYTES = "BYTES";
    private static final String INT = "INT";
    private static final String LONG = "LONG";
    private static final String FLOAT = "FLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String DATE = "DATE";
    private static final String TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String TIMESTAMP_MICROS = "timestamp-micros";
    private static final String NULL = "null";

    static {
        var AVRO = TypeMap.AVRO;

        AVRO.put(STRING, new VarCharType(false, VarCharType.MAX_LENGTH));
        AVRO.put(BYTES, new BinaryType(false, BinaryType.MAX_LENGTH));
        AVRO.put(INT, new IntType(false));
        AVRO.put(LONG, new BigIntType(false));
        AVRO.put(FLOAT, new FloatType(false));
        AVRO.put(DOUBLE, new DoubleType(false));
        AVRO.put(BOOLEAN, new BooleanType(false));
        AVRO.put(DATE, new DateType(false));
        AVRO.put(TIMESTAMP_MILLIS, new TimestampType(false, TimestampType.DEFAULT_PRECISION));
        AVRO.put(TIMESTAMP_MICROS, new TimestampType(false, TimestampType.DEFAULT_PRECISION));
        AVRO.put(NULL, new NullType());
    }

    static {
        var AVRO_NULLABLE = TypeMap.AVRO_NULLABLE;

        AVRO_NULLABLE.put(STRING, new VarCharType(VarCharType.MAX_LENGTH));
        AVRO_NULLABLE.put(BYTES, new BinaryType());
        AVRO_NULLABLE.put(INT, new IntType());
        AVRO_NULLABLE.put(LONG, new BigIntType());
        AVRO_NULLABLE.put(FLOAT, new FloatType());
        AVRO_NULLABLE.put(DOUBLE, new DoubleType());
        AVRO_NULLABLE.put(BOOLEAN, new BooleanType());
        AVRO_NULLABLE.put(DATE, new DateType());
        AVRO_NULLABLE.put(TIMESTAMP_MILLIS, new TimestampType());
        AVRO_NULLABLE.put(TIMESTAMP_MICROS, new TimestampType());
        AVRO_NULLABLE.put(NULL, new NullType());
    }

    static {
        var HUDI = TypeMap.HUDI;

        HUDI.put(STRING, "STRING");
        HUDI.put(BYTES, "BYTES");
        HUDI.put(INT, "INT");
        HUDI.put(LONG, "BIGINT");
        HUDI.put(FLOAT, "FLOAT");
        HUDI.put(DOUBLE, "DOUBLE");
        HUDI.put(BOOLEAN, "BOOLEAN");
        HUDI.put(DATE, "DATE");
        HUDI.put(TIMESTAMP_MILLIS, "TIMESTAMP");
        HUDI.put(TIMESTAMP_MICROS, "TIMESTAMP(3)");
    }

    private TypeMap() {
    }
}
