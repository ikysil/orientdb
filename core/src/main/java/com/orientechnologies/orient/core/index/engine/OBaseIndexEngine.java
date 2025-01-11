package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.IndexEngineData;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.index.OIndexOneValue;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.io.IOException;
import java.util.stream.Stream;

public interface OBaseIndexEngine {
  int getId();

  void init(OIndexMetadata metadata);

  void flush();

  void create(OAtomicOperation atomicOperation, IndexEngineData data) throws IOException;

  void load(IndexEngineData data);

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void clear(OAtomicOperation atomicOperation) throws IOException;

  void close();

  Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      final Object toKey,
      final boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> stream(IndexEngineValuesTransformer valuesTransformer);

  Stream<ORawPair<Object, ORID>> descStream(IndexEngineValuesTransformer valuesTransformer);

  Stream<Object> keyStream();

  long size(IndexEngineValuesTransformer transformer);

  boolean hasRangeQuerySupport();

  int getEngineAPIVersion();

  String getName();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * index engine.
   *
   * <p>
   *
   * <p>If this index engine supports a more narrow locking, for example key-based sharding, it may
   * use the provided {@code key} to infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is
   *     sensitive to the provided {@code key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  String getIndexNameByKey(Object key);

  void updateUniqueIndexVersion(Object key);

  int getUniqueIndexVersion(Object key);

  default boolean hasRidBagTreesSupport() {
    return false;
  }

  void put(OAtomicOperation atomicOperation, Object key, ORID value);

  boolean remove(OAtomicOperation atomicOperation, Object key);

  boolean remove(OAtomicOperation atomicOperation, Object key, ORID value);

  boolean validatedPut(
      OAtomicOperation atomicOperation,
      Object key,
      ORID value,
      IndexEngineValidator<Object, ORID> validator);

  default void applyTxChanges(OAtomicOperation atomicOperation, OTransactionIndexChanges changes) {
    for (final OTransactionIndexChangesPerKey changesPerKey : changes.changesPerKey.values()) {
      applyKeyTxChanges(atomicOperation, changesPerKey, this, changes.getAssociatedIndex());
    }
    applyKeyTxChanges(atomicOperation, changes.nullKeyChanges, this, changes.getAssociatedIndex());
  }

  private static void applyKeyTxChanges(
      OAtomicOperation atomicOperation,
      OTransactionIndexChangesPerKey changes,
      OBaseIndexEngine engine,
      OIndexInternal index) {

    IndexEngineValidator<Object, ORID> uniqueValidator = null;
    if (index.isUnique()) {
      uniqueValidator = ((OIndexOneValue) index).getUniqueValidator();
    }
    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry op :
        index.interpretTxKeyChanges(changes)) {
      switch (op.getOperation()) {
        case PUT:
          if (uniqueValidator != null) {
            engine.validatedPut(
                atomicOperation, changes.key, op.getValue().getIdentity(), uniqueValidator);
          } else {
            engine.put(atomicOperation, changes.key, op.getValue().getIdentity());
          }
          break;
        case REMOVE:
          if (op.getValue() != null) {
            engine.remove(atomicOperation, changes.key, op.getValue().getIdentity());
          } else {
            engine.remove(atomicOperation, changes.key);
          }
          break;
        case CLEAR:
          // SHOULD NEVER BE THE CASE HANDLE BY cleared FLAG
          break;
      }
      engine.updateUniqueIndexVersion(changes.key);
    }
  }
}
