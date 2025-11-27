package io.pixelsdb.pixels.flink;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class PixelsRpcClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcClient.class);

    private final ManagedChannel channel;
    private final PixelsPollingServiceGrpc.PixelsPollingServiceBlockingStub blockingStub;

    public PixelsRpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    PixelsRpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = PixelsPollingServiceGrpc.newBlockingStub(channel);
    }

    public PollResponse pollEvents(String schemaName, String tableName) {
        PollRequest request = PollRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();
        try {
            return blockingStub.pollEvents(request);
        } catch (Exception e) {
            LOG.error("RPC call failed: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        }
    }
}
