package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class IntCodec implements ValueCodec {
  @Nullable
  @Override
  public ByteBuffer encode(@Nullable QueryOuterClass.Value value, @NonNull Column.ColumnType type) {
    if (value.getInnerCase() != InnerCase.INT) {
      throw new IllegalArgumentException("Expected integer type");
    }
    // TODO: Check value range of long value
    return TypeCodecs.INT.encodePrimitive((int) value.getInt(), ProtocolVersion.DEFAULT);
  }

  @NonNull
  @Override
  public QueryOuterClass.Value decode(@Nullable ByteBuffer bytes) {
    return Value.newBuilder()
        .setInt(TypeCodecs.INT.decodePrimitive(bytes, ProtocolVersion.DEFAULT))
        .build();
  }
}
