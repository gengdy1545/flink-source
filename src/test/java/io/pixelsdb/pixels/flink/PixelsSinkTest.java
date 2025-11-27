package io.pixelsdb.pixels.flink;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.ColumnValue;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.OperationType;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollResponse;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowRecord;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowValue;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PixelsSinkTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static Server server;
    private static int port;
    private static final BlockingQueue<PollResponse> responseQueue = new LinkedBlockingQueue<>();

    @BeforeClass
    public static void startServer() throws IOException {
        server = ServerBuilder.forPort(0)
                .addService(new MockPixelsPollingService())
                .build()
                .start();
        port = server.getPort();
        System.out.println("Mock Server started on port " + port);
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPaimonSink() throws Exception {
        runTest("paimon", tempFolder.newFolder("paimon").toURI().toString());
    }

    @Test
    public void testIcebergSink() throws Exception {
        runTest("iceberg", tempFolder.newFolder("iceberg").toURI().toString());
    }

    private void runTest(String sinkType, String warehouse) throws Exception {
        // Push data
        responseQueue.add(createInsertEvent());
        responseQueue.add(createUpdateEvent());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Enable Checkpoint for Sinks
        env.enableCheckpointing(1000);

        Properties props = new Properties();
        props.setProperty("pixels.server.host", "localhost");
        props.setProperty("pixels.server.port", String.valueOf(port));
        props.setProperty("schema.name", "public");
        props.setProperty("table.name", "test_table_" + sinkType);
        props.setProperty("sink.type", sinkType);
        props.setProperty("checkpoint.interval.ms", "1000");
        
        if ("paimon".equals(sinkType)) {
            props.setProperty("paimon.catalog.warehouse", warehouse);
            props.setProperty("paimon.table.name", "paimon_tbl");

            // Create Paimon Table
            Options options = new Options();
            options.set("warehouse", warehouse);
            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            try {
                catalog.createDatabase("default", true);
            } catch (org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException e) {
                // Ignore
            }
            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.column("id", DataTypes.INT());
            schemaBuilder.column("data", DataTypes.STRING());
            schemaBuilder.primaryKey("id");
            catalog.createTable(Identifier.create("default", "paimon_tbl"), schemaBuilder.build(), false);
        } else {
            props.setProperty("iceberg.catalog.type", "hadoop");
            props.setProperty("iceberg.catalog.warehouse", warehouse);
            props.setProperty("iceberg.table.name", "iceberg_tbl");

            // Create Iceberg Table
            Configuration hadoopConf = new Configuration();
            HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "data", Types.StringType.get())
            );
            TableIdentifier name = TableIdentifier.of("default", "iceberg_tbl");
            catalog.createTable(name, schema, PartitionSpec.unpartitioned());
        }

        // Build job graph
        PixelsFlinkApp.buildJob(env, props);
        
        // Run job asynchronously
        JobClient jobClient = env.executeAsync("Test Sink Job");
        
        // Wait a bit for job to run and process events
        Thread.sleep(5000);
        
        // Cancel job
        try {
            jobClient.cancel().get();
        } catch (Exception e) {
            // Expected
        }
    }

    private PollResponse createInsertEvent() {
        RowRecord event = RowRecord.newBuilder()
                .setOp(OperationType.INSERT)
                .setAfter(createRowValue("1", "insert"))
                .build();
        return PollResponse.newBuilder().addEvents(event).build();
    }

    private PollResponse createUpdateEvent() {
        RowRecord event = RowRecord.newBuilder()
                .setOp(OperationType.UPDATE)
                .setBefore(createRowValue("1", "insert"))
                .setAfter(createRowValue("1", "update"))
                .build();
        return PollResponse.newBuilder().addEvents(event).build();
    }

    private RowValue createRowValue(String... values) {
        RowValue.Builder builder = RowValue.newBuilder();
        for (String val : values) {
            builder.addValues(ColumnValue.newBuilder()
                    .setValue(ByteString.copyFromUtf8(val))
                    .build());
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
