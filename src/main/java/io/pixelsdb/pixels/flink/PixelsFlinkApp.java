package io.pixelsdb.pixels.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.Table;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.util.*;

public class PixelsFlinkApp {
    public static void main(String[] args) throws Exception {
        // 1. Load Properties
        Properties props = new Properties();
        try (InputStream input = PixelsFlinkApp.class.getClassLoader().getResourceAsStream("pixels-client.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                System.out.println("Sorry, unable to find pixels-client.properties");
                return;
            }
        }

        // 2. Setup Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing as it is required for some sinks (like Iceberg) to commit data
        env.enableCheckpointing(Long.parseLong(props.getProperty("checkpoint.interval.ms")));

        // 3. Build and Execute Job
        buildJob(env, props);
        
        env.execute("Pixels Flink App");
    }

    public static void buildJob(StreamExecutionEnvironment env, Properties props) {
        RowType rowType = parseSchema(props.getProperty("source.schema"));

        // Create Source
        PixelsRpcSource source = new PixelsRpcSource(props, rowType);
        DataStream<RowData> stream = env.addSource(source).name("PixelsRpcSource");

        // Configure Sink based on type
        String sinkType = props.getProperty("sink.type");
        if ("iceberg".equalsIgnoreCase(sinkType)) {
            configureIcebergSink(stream, props);
        } else {
            configurePaimonSink(stream, props);
        }
    }

    private static void configurePaimonSink(DataStream<RowData> stream, Properties props) {
        String warehouse = props.getProperty("paimon.catalog.warehouse");
        String tableNameStr = props.getProperty("paimon.table.name");
        
        // Parse database and table name
        String dbName = "default";
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        Options options = new Options();
        options.set("warehouse", warehouse);

        try {
            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            org.apache.paimon.table.Table table = catalog.getTable(Identifier.create(dbName, tblName));

            new FlinkSinkBuilder((FileStoreTable) table)
                .withInput(stream)
                .build();
                
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure Paimon sink", e);
        }
    }

    private static void configureIcebergSink(DataStream<RowData> stream, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String warehouse = props.getProperty("iceberg.catalog.warehouse");
        String tableNameStr = props.getProperty("iceberg.table.name");
        
        // Parse database and table name
        String dbName = "default";
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", warehouse);

        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        CatalogLoader catalogLoader;
        Configuration hadoopConf = new Configuration();

        if ("glue".equalsIgnoreCase(catalogType)) {
            // AWS Glue Catalog - credentials auto-discovered from ~/.aws/credentials
            catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            
            catalogLoader = CatalogLoader.custom("glue_catalog", catalogProps, hadoopConf, 
                "org.apache.iceberg.aws.glue.GlueCatalog");
        } else {
            throw new RuntimeException("Unsupported catalog type.");
        }

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
                .append();
    }

    private static RowType parseSchema(String schemaStr) {
        String[] columns = schemaStr.split(",");
        List<String> names = new ArrayList<>();
        List<LogicalType> types = new ArrayList<>();

        for (String col : columns) {
            String[] parts = col.trim().split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid schema format. Expected 'name:type', got: " + col);
            }
            String name = parts[0].trim();
            String typeStr = parts[1].trim().toUpperCase();

            names.add(name);
            types.add(mapType(typeStr));
        }

        return RowType.of(
                types.toArray(new LogicalType[0]),
                names.toArray(new String[0])
        );
    }

    private static LogicalType mapType(String typeStr) {
        switch (typeStr) {
            case "INT":
            case "INTEGER":
                return new IntType();
            case "STRING":
            case "VARCHAR":
                return new VarCharType();
            case "BIGINT":
            case "LONG":
                return new BigIntType();
            case "FLOAT":
                return new FloatType();
            case "DOUBLE":
                return new DoubleType();
            case "BOOLEAN":
            case "BOOL":
                return new BooleanType();
            default:
                throw new UnsupportedOperationException("Unsupported type in config: " + typeStr);
        }
    }
}
