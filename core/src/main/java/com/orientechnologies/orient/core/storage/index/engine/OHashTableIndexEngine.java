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
package com.orientechnologies.orient.core.storage.index.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.LocalHashTableV2;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v3.OLocalHashTableV3;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 15.07.13
 */
public final class OHashTableIndexEngine implements OIndexEngine {
  public static final int VERSION = 3;

  public static final String METADATA_FILE_EXTENSION = ".him";
  public static final String TREE_FILE_EXTENSION = ".hit";
  public static final String BUCKET_FILE_EXTENSION = ".hib";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final OHashTable<Object, Object> hashTable;
  private final AtomicLong bonsayFileId = new AtomicLong(0);

  @SuppressWarnings("rawtypes")
  private volatile OBinarySerializer keySerializer;

  private volatile OType[] keyTypes;

  private final String name;

  private final int id;

  public OHashTableIndexEngine(
      String name, int id, OAbstractPaginatedStorage storage, int version) {
    this.id = id;
    if (version < 2) {
      throw new IllegalStateException("Unsupported version of hash index");
    } else if (version == 2) {
      hashTable =
          new LocalHashTableV2<>(
              name,
              METADATA_FILE_EXTENSION,
              TREE_FILE_EXTENSION,
              BUCKET_FILE_EXTENSION,
              NULL_BUCKET_FILE_EXTENSION,
              storage);
    } else if (version == 3) {
      hashTable =
          new OLocalHashTableV3<>(
              name,
              METADATA_FILE_EXTENSION,
              TREE_FILE_EXTENSION,
              BUCKET_FILE_EXTENSION,
              NULL_BUCKET_FILE_EXTENSION,
              storage);
    } else {
      throw new IllegalStateException("Invalid value of the index version , version = " + version);
    }

    this.name = name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {}

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void create(
      OAtomicOperation atomicOperation,
      @SuppressWarnings("rawtypes") OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      @SuppressWarnings("rawtypes") OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption)
      throws IOException {
    final OHashFunction<Object> hashFunction;

    this.keySerializer = keySerializer;
    this.keyTypes = keyTypes;

    if (encryption != null) {
      //noinspection unchecked,rawtypes
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      //noinspection unchecked, rawtypes
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }

    //noinspection unchecked
    hashTable.create(
        atomicOperation,
        keySerializer,
        valueSerializer,
        keyTypes,
        encryption,
        hashFunction,
        nullPointerSupport);
  }

  @Override
  public void flush() {}

  @Override
  public String getIndexNameByKey(final Object key) {
    return name;
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) throws IOException {
    doClearTable(atomicOperation);

    hashTable.delete(atomicOperation);
  }

  private void doClearTable(OAtomicOperation atomicOperation) throws IOException {
    final OHashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();

    if (firstEntry != null) {
      OHashTable.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);
      while (entries.length > 0) {
        for (final OHashTable.Entry<Object, Object> entry : entries) {
          hashTable.remove(atomicOperation, entry.key);
        }

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }
    }

    if (hashTable.isNullKeyIsSupported()) {
      hashTable.remove(atomicOperation, null);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void load(
      String indexName,
      @SuppressWarnings("rawtypes") OBinarySerializer valueSerializer,
      boolean isAutomatic,
      @SuppressWarnings("rawtypes") OBinarySerializer keySerializer,
      OType[] keyTypes,
      boolean nullPointerSupport,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption) {

    this.keySerializer = keySerializer;
    this.keyTypes = keyTypes;

    final OHashFunction<Object> hashFunction;

    if (encryption != null) {
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }

    hashTable.load(
        indexName,
        keyTypes,
        nullPointerSupport,
        encryption,
        hashFunction,
        keySerializer,
        valueSerializer);
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) throws IOException {
    return hashTable.remove(atomicOperation, key) != null;
  }

  @Override
  public boolean rawRemove(OAtomicOperation atomicOperation, byte[] rawKey) throws IOException {
    final Object key;
    if (rawKey == null) {
      key = null;
    } else {
      key = keySerializer.deserializeNativeObject(rawKey, 0);
    }

    return remove(atomicOperation, key);
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) throws IOException {
    doClearTable(atomicOperation);
  }

  @Override
  public void close() {
    hashTable.close();
  }

  @Override
  public Object get(Object key) {
    return hashTable.get(key);
  }

  @Override
  public ORawPair<byte[], Object> getRawEntry(final Object key) {
    return new ORawPair<>(serializeKey(key), hashTable.get(key));
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, Object value) throws IOException {
    hashTable.put(atomicOperation, key, value);
  }

  @Override
  public void update(OAtomicOperation atomicOperation, Object key, OIndexKeyUpdater<Object> updater)
      throws IOException {
    Object value = get(key);
    OIndexUpdateAction<Object> updated = updater.update(value, bonsayFileId);
    if (updated.isChange()) {
      put(atomicOperation, key, updated.getValue());
    } else if (updated.isRemove()) {
      remove(atomicOperation, key);
    } else //noinspection StatementWithEmptyBody
    if (updated.isNothing()) {
      // Do nothing
    }
  }

  @Override
  public void updateRaw(OAtomicOperation atomicOperation, byte[] rawKey, OIndexKeyUpdater<Object> update)
          throws IOException {
    final Object key;
    if (rawKey == null) {
      key = null;
    } else {
      key = keySerializer.deserializeNativeObject(rawKey, 0);
    }

    update(atomicOperation, key, update);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator)
      throws IOException {
    //noinspection rawtypes
    return hashTable.validatedPut(atomicOperation, key, value, (Validator) validator);
  }

  @Override
  public long size(ValuesTransformer transformer) {
    if (transformer == null) {
      return hashTable.size();
    } else {
      long counter = 0;

      if (hashTable.isNullKeyIsSupported()) {
        final Object nullValue = hashTable.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      OHashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
      if (firstEntry == null) {
        return counter;
      }

      OHashTable.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);

      while (entries.length > 0) {
        for (OHashTable.Entry<Object, Object> entry : entries) {
          counter += transformer.transformFromValue(entry.value).size();
        }

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }

      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public Stream<ORID> iterateBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateBetween");
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateBetweenRawEntries(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateBetween");
  }

  @Override
  public Stream<ORID> iterateMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateMajor");
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateMajorRawEntries(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateMajor");
  }

  @Override
  public Stream<ORID> iterateMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateMinor");
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateMinorRawEntries(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateMinor");
  }

  @Override
  public Stream<ORID> stream(final ValuesTransformer valuesTransformer) {
    return doStream(valuesTransformer).map(pair -> pair.second);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> rawStream(ValuesTransformer valuesTransformer) {
    return doStream(valuesTransformer)
        .map(pair -> new ORawPair<>(serializeKey(pair.first), pair.second));
  }

  private Stream<ORawPair<Object, ORID>> doStream(final ValuesTransformer valuesTransformer) {
    return StreamSupport.stream(
        new Spliterator<ORawPair<Object, ORID>>() {
          private int nextEntriesIndex;
          private OHashTable.Entry<Object, Object>[] entries;

          private Iterator<ORID> currentIterator = new OEmptyIterator<>();
          private Object currentKey;

          {
            OHashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
            if (firstEntry == null) {
              //noinspection unchecked
              entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              entries = hashTable.ceilingEntries(firstEntry.key);
            }

            if (entries.length == 0) {
              currentIterator = null;
            }
          }

          @Override
          public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
            if (currentIterator == null) {
              return false;
            }

            if (currentIterator.hasNext()) {
              final OIdentifiable identifiable = currentIterator.next();
              action.accept(new ORawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            while (currentIterator != null && !currentIterator.hasNext()) {
              if (entries.length == 0) {
                currentIterator = null;
                return false;
              }

              final OHashTable.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

              currentKey = bucketEntry.key;

              Object value = bucketEntry.value;
              if (valuesTransformer != null) {
                currentIterator = valuesTransformer.transformFromValue(value).iterator();
              } else {
                currentIterator = Collections.singletonList((ORID) value).iterator();
              }

              nextEntriesIndex++;

              if (nextEntriesIndex >= entries.length) {
                entries = hashTable.higherEntries(entries[entries.length - 1].key);

                nextEntriesIndex = 0;
              }
            }

            if (currentIterator != null) {
              final OIdentifiable identifiable = currentIterator.next();
              action.accept(new ORawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            return false;
          }

          @Override
          public Spliterator<ORawPair<Object, ORID>> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return NONNULL;
          }
        },
        false);
  }

  @Override
  public Stream<ORID> descStream(final ValuesTransformer valuesTransformer) {
    return doDescStream(valuesTransformer).map(pair -> pair.second);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> rawDescStream(ValuesTransformer valuesTransformer) {
    return doDescStream(valuesTransformer)
        .map(pair -> new ORawPair<>(serializeKey(pair.first), pair.second));
  }

  private Stream<ORawPair<Object, ORID>> doDescStream(final ValuesTransformer valuesTransformer) {
    return StreamSupport.stream(
        new Spliterator<ORawPair<Object, ORID>>() {
          private int nextEntriesIndex;
          private OHashTable.Entry<Object, Object>[] entries;

          private Iterator<ORID> currentIterator = new OEmptyIterator<>();
          private Object currentKey;

          {
            OHashTable.Entry<Object, Object> lastEntry = hashTable.lastEntry();
            if (lastEntry == null) {
              //noinspection unchecked
              entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              entries = hashTable.floorEntries(lastEntry.key);
            }

            if (entries.length == 0) {
              currentIterator = null;
            }
          }

          @Override
          public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
            if (currentIterator == null) {
              return false;
            }

            if (currentIterator.hasNext()) {
              final OIdentifiable identifiable = currentIterator.next();
              action.accept(new ORawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            while (currentIterator != null && !currentIterator.hasNext()) {
              if (entries.length == 0) {
                currentIterator = null;
                return false;
              }

              final OHashTable.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

              currentKey = bucketEntry.key;

              Object value = bucketEntry.value;
              if (valuesTransformer != null) {
                currentIterator = valuesTransformer.transformFromValue(value).iterator();
              } else {
                currentIterator = Collections.singletonList((ORID) value).iterator();
              }

              nextEntriesIndex--;

              if (nextEntriesIndex < 0) {
                entries = hashTable.lowerEntries(entries[0].key);

                nextEntriesIndex = entries.length - 1;
              }
            }

            if (currentIterator != null) {
              final OIdentifiable identifiable = currentIterator.next();
              action.accept(new ORawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            return false;
          }

          @Override
          public Spliterator<ORawPair<Object, ORID>> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return NONNULL;
          }
        },
        false);
  }

  private byte[] serializeKey(final Object key) {
    if (key == null) {
      return null;
    }

    //noinspection unchecked
    return keySerializer.serializeNativeAsWhole(key, (Object[]) keyTypes);
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    hashTable.acquireAtomicExclusiveLock();
    return true;
  }
}
