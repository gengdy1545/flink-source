package io.pixelsdb.pixels.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
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
import java.util.ArrayList;
import java.util.Properties;

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
        // Define Schema (Hardcoded for demo)
        RowType rowType = RowType.of(
                new LogicalType[]{new IntType(), new VarCharType()},
                new String[]{"id", "data"}
        );

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

        // Initialize Iceberg Catalog (using HadoopCatalog for simplicity as per properties default)
        Configuration hadoopConf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);

        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);
        Table table = catalog.loadTable(identifier);

        FlinkSink.forRowData(stream)
                .table(table)
                .equalityFieldColumns(new ArrayList<>(table.schema().identifierFieldNames()))
                .append();
    }
}
