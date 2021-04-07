package io.stargate.grpc.server;

import io.grpc.stub.StreamObserver;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Rows;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.proto.QueryOuterClass.Empty;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;

public class GrpcServer extends io.stargate.proto.StargateGrpc.StargateImplBase {
  private final Persistence persistence;

  public GrpcServer(Persistence persistence) {
    this.persistence = persistence;
  }

  @Override
  public void execute(Query query, StreamObserver<Result> responseObserver) {
    try {
      long queryStartNanoTime = System.nanoTime();
      QueryParameters parameters = query.getParameters();
      Payload payload = parameters.getPayload();

      Payload.Type payloadType = payload.getType() == null ? Type.TYPE_CQL : payload.getType();

      PayloadHandler handler = PayloadHandlers.HANDLERS.get(payloadType);
      if (handler == null) {
        responseObserver.onError(new IllegalArgumentException("Unsupported payload type"));
      }

      Connection connection = persistence.newConnection();
      connection
          .prepare(
              query.getCql(),
              ImmutableParameters.builder()
                  .tracingRequested(query.getParameters().getTracing())
                  .build())
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  // TODO: Do something with error. Maybe send error in response instead.
                  responseObserver.onError(t);
                } else {
                  try {
                    connection
                        .execute(
                            handler.bindValues(prepared, payload),
                            ImmutableParameters.builder().build(), // TODO: Build parameters
                            queryStartNanoTime)
                        .whenComplete(
                            (result, t2) -> {
                              if (t2 != null) {
                                // TODO: Do something with error.
                                responseObserver.onError(t2);
                              } else {
                                Result.Builder resultBuilder = Result.newBuilder();
                                switch (result.kind) {
                                  case Void:
                                    resultBuilder.setEmpty(Empty.newBuilder().build());
                                    break;
                                  case Rows:
                                    resultBuilder.setPayload(handler.processResult((Rows) result));
                                    break;
                                  case SchemaChange:
                                    // TODO: Wait for schema agreement, etc.
                                    persistence
                                        .waitForSchemaAgreement(); // TODO: Could this be made
                                    // async? This is blocking the
                                    // gRPC thread.
                                    resultBuilder.setEmpty(Empty.newBuilder().build());
                                    break;
                                  case SetKeyspace:
                                    // TODO: Prevent "USE <keyspace>" from happening
                                    throw new RuntimeException("USE <keyspace> not supported");
                                  default:
                                    throw new RuntimeException("Unhandled result kind");
                                }
                                responseObserver.onNext(resultBuilder.build());
                                responseObserver.onCompleted();
                              }
                            });
                  } catch (Exception e) {
                    // TODO: Do something with error.
                    responseObserver.onError(e);
                  }
                }
              });
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
