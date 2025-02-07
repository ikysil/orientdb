/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.stream.Streams;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.comparator.AscComparator;
import com.orientechnologies.orient.core.index.comparator.DescComparator;
import com.orientechnologies.orient.core.index.iterator.PureTxMultiValueBetweenIndexBackwardSplititerator;
import com.orientechnologies.orient.core.index.iterator.PureTxMultiValueBetweenIndexForwardSpliterator;
import com.orientechnologies.orient.core.index.multivalue.MultiValuesTransformer;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract index implementation that supports multi-values for the same key.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexMultiValues extends OIndexAbstract {

  OIndexMultiValues(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Deprecated
  @Override
  public Collection<ORID> get(Object key) {
    final List<ORID> rids;
    try (Stream<ORID> stream = getRids(key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  @Override
  public Stream<ORID> getRidsIgnoreTx(Object key) {
    final Object collatedKey = getCollatingValue(key);
    Stream<ORID> backedStream;
    acquireSharedLock();
    try {
      Stream<ORID> stream;
      while (true) {
        try {
          if (apiVersion == 0) {
            //noinspection unchecked
            final Collection<ORID> values =
                (Collection<ORID>) storage.getIndexValue(indexId, collatedKey);
            if (values != null) {
              //noinspection resource
              stream = values.stream();
            } else {
              //noinspection resource
              stream = Stream.empty();
            }
          } else if (apiVersion == 1) {
            //noinspection resource
            stream = storage.getIndexValues(indexId, collatedKey);
          } else {
            throw new IllegalStateException("Invalid version of index API - " + apiVersion);
          }
          backedStream = IndexStreamSecurityDecorator.decorateRidStream(this, stream);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    return backedStream;
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    final Object collatedKey = getCollatingValue(key);
    Stream<ORID> backedStream = getRidsIgnoreTx(key);
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return backedStream;
    }
    Set<OIdentifiable> txChanges = calculateTxValue(collatedKey, indexChanges);
    if (txChanges == null) {
      txChanges = Collections.emptySet();
    }
    return IndexStreamSecurityDecorator.decorateRidStream(
        this,
        Stream.concat(
            backedStream
                .map((rid) -> calculateTxIndexEntry(collatedKey, rid, indexChanges))
                .filter(Objects::nonNull)
                .map((pair) -> pair.second),
            txChanges.stream().map(OIdentifiable::getIdentity)));
  }

  public OIndexMultiValues put(Object key, final OIdentifiable singleValue) {
    final ORID rid = singleValue.getIdentity();

    if (!rid.isValid()) {
      if (singleValue instanceof ORecord) {
        // EARLY SAVE IT
        ((ORecord) singleValue).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }

    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, singleValue);
    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, singleValue);
      database.commit();
    }
    return this;
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, key, value);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, key, value);
      database.commit();
    }
    return true;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesBetween(
                      indexId,
                      fromKey,
                      fromInclusive,
                      toKey,
                      toInclusive,
                      ascOrder,
                      MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    ODatabaseDocumentInternal database = getDatabase();

    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream;
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMajor(
                      indexId, fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();

    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, ORID>> stream;

    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMinor(
                      indexId, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder) {
      comparator = ODefaultComparator.INSTANCE;
    } else {
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);
    }

    sortedKeys.sort(comparator);

    Stream<ORawPair<Object, ORID>> stream =
        IndexStreamSecurityDecorator.decorateStream(
            this, sortedKeys.stream().flatMap(this::streamForKey));

    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }
    Comparator<ORawPair<Object, ORID>> keyComparator;
    if (ascSortOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }

    final Stream<ORawPair<Object, ORID>> txStream =
        keys.stream()
            .flatMap((key) -> txStramForKey(indexChanges, key))
            .filter(Objects::nonNull)
            .sorted(keyComparator);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder));
  }

  private Stream<ORawPair<Object, ORID>> txStramForKey(
      final OTransactionIndexChanges indexChanges, Object key) {
    final Set<OIdentifiable> result = calculateTxValue(getCollatingValue(key), indexChanges);
    if (result != null) {
      return result.stream()
          .map((rid) -> new ORawPair<>(getCollatingValue(key), rid.getIdentity()));
    }
    return null;
  }

  private Stream<ORawPair<Object, ORID>> streamForKey(Object key) {
    key = getCollatingValue(key);

    final Object entryKey = key;
    acquireSharedLock();
    try {
      while (true) {
        try {
          if (apiVersion == 0) {
            //noinspection unchecked,resource
            return Optional.ofNullable((Collection<ORID>) storage.getIndexValue(indexId, key))
                .map((rids) -> rids.stream().map((rid) -> new ORawPair<>(entryKey, rid)))
                .orElse(Stream.empty());
          } else if (apiVersion == 1) {
            //noinspection resource
            return storage.getIndexValues(indexId, key).map((rid) -> new ORawPair<>(entryKey, rid));
          } else {
            throw new IllegalStateException("Invalid version of index API - " + apiVersion);
          }
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  public static Set<OIdentifiable> calculateTxValue(
      final Object key, OTransactionIndexChanges indexChanges) {
    final List<OIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return null;
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) result.clear();
        else result.remove(entry.getValue());
      } else result.add(entry.getValue());
    }

    if (result.isEmpty()) return null;

    return new HashSet<>(result);
  }

  public long size() {
    acquireSharedLock();
    long tot;
    try {
      while (true) {
        try {
          tot = storage.getIndexSize(indexId, MultiValuesTransformer.INSTANCE);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChanges(getName());
    if (indexChanges != null) {
      try (Stream<ORawPair<Object, ORID>> stream = stream()) {
        return stream.count();
      }
    }

    return tot;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexStream(indexId, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream =
        StreamSupport.stream(
            new PureTxMultiValueBetweenIndexForwardSpliterator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true));
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream,
      Stream<ORawPair<Object, ORID>> backedStream,
      boolean ascOrder) {
    Comparator<ORawPair<Object, ORID>> keyComparator;
    if (ascOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }
    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map((entry) -> calculateTxIndexEntry(entry.first, entry.second, indexChanges))
            .filter(Objects::nonNull),
        keyComparator);
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.PUT && entry.getValue().equals(backendValue))
        putCounter++;
      else if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) putCounter = 0;
        else if (entry.getValue().equals(backendValue) && putCounter > 0) putCounter--;
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexDescStream(indexId, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream =
        StreamSupport.stream(
            new PureTxMultiValueBetweenIndexBackwardSplititerator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false));
  }
}
