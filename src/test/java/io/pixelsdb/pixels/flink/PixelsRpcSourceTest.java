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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class PixelsRpcSourceTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    private static Server server;
    private static int port;
    private static final BlockingQueue<PollResponse> responseQueue = new LinkedBlockingQueue<>();
    
    // Static collection for results
    private static final List<String> collectedResults = Collections.synchronizedList(new ArrayList<>());

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
    public void testRpcSource() throws Exception {
        collectedResults.clear();
        
        // Prepare data
        // Insert: id=1, data="test_insert"
        RowRecord insertEvent = RowRecord.newBuilder()
                .setOp(OperationType.INSERT)
                .setAfter(createRowValue("1", "test_insert"))
                .build();
        
        // Update: id=1, data="test_insert" -> "test_update"
        RowRecord updateEvent = RowRecord.newBuilder()
                .setOp(OperationType.UPDATE)
                .setBefore(createRowValue("1", "test_insert"))
                .setAfter(createRowValue("1", "test_update"))
                .build();

        responseQueue.add(PollResponse.newBuilder().addEvents(insertEvent).build());
        responseQueue.add(PollResponse.newBuilder().addEvents(updateEvent).build());
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("pixels.server.host", "localhost");
        props.setProperty("pixels.server.port", String.valueOf(port));
        props.setProperty("schema.name", "public");
        props.setProperty("table.name", "test_table");

        RowType rowType = RowType.of(
                new LogicalType[]{new IntType(), new VarCharType()},
                new String[]{"id", "data"}
        );

        DataStream<RowData> stream = env.addSource(new PixelsRpcSource(props, rowType));
        
        stream.addSink(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData row, Context context) {
                String res = row.getRowKind() + ": " + row.getInt(0) + ", " + row.getString(1);
                collectedResults.add(res);
            }
        });
        
        JobClient jobClient = env.executeAsync("Test Job");
        
        // Wait for results
        long deadline = System.currentTimeMillis() + 10000; // 10 seconds timeout
        while (collectedResults.size() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        
        jobClient.cancel().get();

        assertEquals(3, collectedResults.size());
        assertEquals("INSERT: 1, test_insert", collectedResults.get(0));
        assertEquals("UPDATE_BEFORE: 1, test_insert", collectedResults.get(1));
        assertEquals("UPDATE_AFTER: 1, test_update", collectedResults.get(2));
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
                response = PollResponse.newBuilder().build(); // Empty response
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
