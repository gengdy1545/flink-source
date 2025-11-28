package io.pixelsdb.pixels.flink;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.ColumnValue;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.OperationType;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollResponse;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowRecord;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowValue;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-End Integration Test for Pixels Flink App against real AWS S3/Glue.
 * <p>
 * REQUIRES:
 * 1. Valid AWS Credentials in environment (Env Vars or ~/.aws/credentials).
 * 2. Correct S3 bucket configured in pixels-client.properties.
 * </p>
 */
public class PixelsS3ITCase {
    // Flink Mini Cluster
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    // Mock Pixels Server components
    private static Server server;
    private static int port;
    private static final BlockingQueue<PollResponse> responseQueue = new LinkedBlockingQueue<>();

    @BeforeClass
    public static void startServer() throws IOException {
        // Start a mock gRPC server to simulate Pixels data source
        server = ServerBuilder.forPort(0)
                .addService(new MockPixelsPollingService())
                .build()
                .start();
        port = server.getPort();
        System.out.println("Mock Pixels Server started on port " + port);
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testS3Pipeline() throws Exception {
        // 1. Load Config
        Properties props = new Properties();
        try (InputStream input = PixelsS3ITCase.class.getClassLoader().getResourceAsStream("pixels-client.properties")) {
            if (input == null) throw new RuntimeException("pixels-client.properties not found");
            props.load(input);
        }

        // 2. Prepare Unique Table Name for this test run to avoid collision
        String originalTable = props.getProperty("iceberg.table.name", "iceberg_orders");
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String testTableName = originalTable + "_it_" + testId;
        
        // Override properties for Test
        props.setProperty("pixels.server.host", "localhost");
        props.setProperty("pixels.server.port", String.valueOf(port));
        // Force fast checkpointing to make data visible on S3 quickly
        props.setProperty("checkpoint.interval.ms", "2000");
        // Match schema to test data
        props.setProperty("source.schema", "id:INT,data:STRING");

        String sinkType = props.getProperty("sink.type", "iceberg");
        if ("iceberg".equalsIgnoreCase(sinkType)) {
            props.setProperty("iceberg.table.name", testTableName);
            System.out.println("Testing Iceberg Sink with table: " + testTableName);
        } else {
            String paimonTable = props.getProperty("paimon.table.name", "paimon_orders");
            testTableName = paimonTable + "_it_" + testId;
            props.setProperty("paimon.table.name", testTableName);
            System.out.println("Testing Paimon Sink with table: " + testTableName);
        }

        // Create the table on S3/Glue before running the job
        createTableOnS3(sinkType, props, testTableName);

        // 3. Prepare Mock Data
        prepareData();

        // 4. Run Flink Job (The Writer)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000); // 2s Checkpoint
        
        // Build the job using the exact same logic as main()
        PixelsFlinkApp.buildJob(env, props);
        
        JobClient jobClient = env.executeAsync("Pixels S3 Integration Test");

        // 5. Verify Data (The Reader)
        try {
            // Wait for Flink to run and checkpoint at least once
            verifyDataOnS3(sinkType, props, testTableName);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void createTableOnS3(String sinkType, Properties props, String fullTableName) {
        System.out.println("Creating table " + fullTableName + " on S3 for " + sinkType);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String catalogName = "init_cat";
        StringBuilder ddl = new StringBuilder();

        if ("iceberg".equalsIgnoreCase(sinkType)) {
            String warehouse = props.getProperty("iceberg.catalog.warehouse");
            String catalogType = props.getProperty("iceberg.catalog.type", "glue");

            ddl.append("CREATE CATALOG ").append(catalogName).append(" WITH (");
            ddl.append("'type'='iceberg', ");
            ddl.append("'warehouse'='").append(warehouse).append("', ");
            ddl.append("'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', ");
            if ("glue".equalsIgnoreCase(catalogType)) {
                ddl.append("'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog'");
                if (props.containsKey("iceberg.catalog.glue.region")) {
                    ddl.append(", 'glue.region'='").append(props.getProperty("iceberg.catalog.glue.region")).append("'");
                }
            } else {
                ddl.append("'catalog-type'='hadoop'");
            }
            ddl.append(")");
        } else {
            String warehouse = props.getProperty("paimon.catalog.warehouse");
            ddl.append("CREATE CATALOG ").append(catalogName).append(" WITH (");
            ddl.append("'type'='paimon', ");
            ddl.append("'warehouse'='").append(warehouse).append("'");
            ddl.append(")");
        }

        tEnv.executeSql(ddl.toString());
        tEnv.executeSql("USE CATALOG " + catalogName);

        String dbName = "default";
        String tblName = fullTableName;
        if (fullTableName.contains(".")) {
            String[] parts = fullTableName.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }
        if (!"default".equalsIgnoreCase(dbName)) {
             tEnv.executeSql("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
        }
        tEnv.executeSql("USE `" + dbName + "`");

        String createTableSql;
        if ("iceberg".equalsIgnoreCase(sinkType)) {
             String warehouse = props.getProperty("iceberg.catalog.warehouse");
             String tableLocation = warehouse + (warehouse.endsWith("/") ? "" : "/") + tblName;
             createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                " id INT, " +
                " data STRING, " +
                " PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                " 'format-version'='2', " +
                " 'write.upsert.enabled'='true'," +
                " 'location'='%s'" +
                ")", tblName, tableLocation);
        } else {
             createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                " id INT PRIMARY KEY NOT ENFORCED, " +
                " data STRING" +
                ")", tblName);
        }
        tEnv.executeSql(createTableSql);
    }

    private void verifyDataOnS3(String sinkType, Properties props, String fullTableName) throws Exception {
        System.out.println("Starting verification for " + sinkType);
        
        // Setup a separate Flink Environment for reading back data
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String catalogName = "verify_cat";
        StringBuilder ddl = new StringBuilder();

        // Reconstruct the Catalog DDL to match what the App uses
        // verification MUST use the same S3/Glue interfaces to succeed.
        if ("iceberg".equalsIgnoreCase(sinkType)) {
            String warehouse = props.getProperty("iceberg.catalog.warehouse");
            String catalogType = props.getProperty("iceberg.catalog.type", "glue");

            ddl.append("CREATE CATALOG ").append(catalogName).append(" WITH (");
            ddl.append("'type'='iceberg', ");
            ddl.append("'warehouse'='").append(warehouse).append("', ");
            
            // KEY: Use the same Standard S3 Interface for verification
            ddl.append("'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', ");
            
            if ("glue".equalsIgnoreCase(catalogType)) {
                ddl.append("'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog'");
                if (props.containsKey("iceberg.catalog.glue.region")) {
                    ddl.append(", 'glue.region'='").append(props.getProperty("iceberg.catalog.glue.region")).append("'");
                }
            } else {
                ddl.append("'catalog-type'='hadoop'"); // Fallback if user changed config
            }
            ddl.append(")");

        } else {
            // Paimon Verification
            String warehouse = props.getProperty("paimon.catalog.warehouse");
            
            ddl.append("CREATE CATALOG ").append(catalogName).append(" WITH (");
            ddl.append("'type'='paimon', ");
            ddl.append("'warehouse'='").append(warehouse).append("'");
            // Paimon automatically handles s3:// if jars are present, no extra IO impl needed usually
            ddl.append(")");
        }

        System.out.println("Executing DDL: " + ddl);
        tEnv.executeSql(ddl.toString());
        tEnv.executeSql("USE CATALOG " + catalogName);

        // Parse DB and Table
        String dbName = "default";
        String tblName = fullTableName;
        if (fullTableName.contains(".")) {
            String[] parts = fullTableName.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }
        tEnv.executeSql("USE `" + dbName + "`");

        // Poll for data availability (S3 is eventually consistent + Flink Checkpoint delay)
        List<String> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 120_000; // 2 minutes timeout for S3/Glue
        boolean found = false;

        while (System.currentTimeMillis() < deadline) {
            try {
                TableResult result = tEnv.executeSql("SELECT id, data FROM " + tblName);
                results.clear();
                try (CloseableIterator<Row> it = result.collect()) {
                    while (it.hasNext()) {
                        Row row = it.next();
                        results.add(row.getField(0) + "=" + row.getField(1));
                    }
                }

                System.out.println("Polled rows: " + results);

                // Check expected data:
                // 1=val1_updated (after update)
                // 2=val2 (insert)
                if (results.contains("1=val1_updated") && results.contains("2=val2") && results.size() == 2) {
                    found = true;
                    break;
                }
            } catch (Exception e) {
                System.out.println("Query failed (table might not exist yet): " + e.getMessage());
            }
            Thread.sleep(5000);
        }

        if (!found) {
            throw new AssertionError("Timeout waiting for data on S3. Found: " + results);
        }
        System.out.println("Verification Successful! Data found on S3: " + results);
    }

    private void prepareData() {
        responseQueue.clear();
        // 1. Insert ID 1
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder().setOp(OperationType.INSERT)
                        .setAfter(createRowValue("1", "val1")).build()
        ).build());

        // 2. Insert ID 2
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder().setOp(OperationType.INSERT)
                        .setAfter(createRowValue("2", "val2")).build()
        ).build());

        // 3. Update ID 1 -> val1_updated
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder().setOp(OperationType.UPDATE)
                        .setBefore(createRowValue("1", "val1"))
                        .setAfter(createRowValue("1", "val1_updated")).build()
        ).build());
    }

    private RowValue createRowValue(String... values) {
        RowValue.Builder builder = RowValue.newBuilder();
        for (String val : values) {
            builder.addValues(ColumnValue.newBuilder()
                    .setValue(ByteString.copyFromUtf8(val)).build());
        }
        return builder.build();
    }

    static class MockPixelsPollingService extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase {
        @Override
        public void pollEvents(PollRequest request, StreamObserver<PollResponse> responseObserver) {
            PollResponse response = responseQueue.poll();
            if (response == null) {
                response = PollResponse.newBuilder().build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
