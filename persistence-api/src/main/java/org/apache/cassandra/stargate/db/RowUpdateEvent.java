package org.apache.cassandra.stargate.db;

import java.util.List;

/** Represents an update affecting a single row. */
public interface RowUpdateEvent extends MutationEvent {

  List<Cell> getCells();
}
