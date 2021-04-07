package io.stargate.it.grpc;

import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import org.junit.jupiter.api.Test;

public class SimpleQuery extends BaseOsgiIntegrationTest {
  @Test
  public void simpleQueryTest(StargateEnvironmentInfo stargate) {
    // TODO: Consider reusing channel in multiple tests
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(stargate.nodes().get(0).seedAddress(), 8090)
            .usePlaintext()
            .build();
    StargateGrpc.StargateBlockingStub stub = StargateGrpc.newBlockingStub(channel);
    // TODO: Get and check result
    stub.execute(
        Query.newBuilder()
            .setCql("SELECT * FROM system.local WHERE key = ?")
            .setParameters(
                QueryParameters.newBuilder()
                    .setPayload(
                        Payload.newBuilder()
                            .setType(Type.TYPE_CQL)
                            .setValue(
                                Any.pack(
                                    Values.newBuilder()
                                        .addValues(Value.newBuilder().setString("local").build())
                                        .build()))
                            .build())
                    .build())
            .build());
  }
}
