/* Generated By:JJTree: Do not edit this line. OWhereClause.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OWhereClause extends SimpleNode {
  protected OBooleanExpression baseExpression;

  private List<OAndBlock> flattened;

  public OWhereClause(int id) {
    super(id);
  }

  public OWhereClause(OrientSql p, int id) {
    super(p, id);
  }

  public boolean matchesFilters(OIdentifiable currentRecord, OCommandContext ctx) {
    return matchesFilters(new OResultInternal(currentRecord), ctx);
  }

  public boolean matchesFilters(OResult currentRecord, OCommandContext ctx) {
    if (baseExpression == null) {
      return true;
    }
    return baseExpression.evaluate(currentRecord, ctx);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (baseExpression == null) {
      return;
    }
    baseExpression.toString(params, builder);
  }

  public void toGenericStatement(StringBuilder builder) {
    if (baseExpression == null) {
      return;
    }
    baseExpression.toGenericStatement(builder);
  }

  /**
   * estimates how many items of this class will be returned applying this filter
   *
   * @return an estimation of the number of records of this class returned applying this filter, 0
   *     if and only if sure that no records are returned
   */
  public long estimate(OClass oClass, long threshold, OCommandContext ctx) {
    long count = oClass.count();
    if (count > 1) {
      count = count / 2;
    }
    if (count < threshold) {
      return count;
    }

    long indexesCount = 0L;
    List<OAndBlock> flattenedConditions = flatten();
    Set<OIndex> indexes = oClass.getIndexes();
    for (OAndBlock condition : flattenedConditions) {

      List<OBinaryCondition> indexedFunctConditions =
          condition.getIndexedFunctionConditions(
              oClass, (ODatabaseDocumentInternal) ctx.getDatabase());

      long conditionEstimation = Long.MAX_VALUE;

      if (indexedFunctConditions != null) {
        for (OBinaryCondition cond : indexedFunctConditions) {
          OFromClause from = new OFromClause(-1);
          from.item = new OFromItem(-1);
          from.item.setIdentifier(new OIdentifier(oClass.getName()));
          long newCount = cond.estimateIndexed(from, ctx);
          if (newCount < conditionEstimation) {
            conditionEstimation = newCount;
          }
        }
      } else {
        Map<String, Object> conditions = getEqualityOperations(condition, ctx);

        for (OIndex index : indexes) {
          if (index.getType().equals(OClass.INDEX_TYPE.FULLTEXT.name())) {
            continue;
          }
          List<String> indexedFields = index.getDefinition().getFields();
          int nMatchingKeys = 0;
          for (String indexedField : indexedFields) {
            if (conditions.containsKey(indexedField)) {
              nMatchingKeys++;
            } else {
              break;
            }
          }
          if (nMatchingKeys > 0) {
            long newCount = estimateFromIndex(index, conditions, nMatchingKeys);
            if (newCount < conditionEstimation) {
              conditionEstimation = newCount;
            }
          }
        }
      }
      if (conditionEstimation > count) {
        return count;
      }
      indexesCount += conditionEstimation;
    }
    return Math.min(indexesCount, count);
  }

  private static long estimateFromIndex(
      OIndex index, Map<String, Object> conditions, int nMatchingKeys) {
    if (nMatchingKeys < 1) {
      throw new IllegalArgumentException("Cannot estimate from an index with zero keys");
    }
    OIndexDefinition definition = index.getDefinition();
    List<String> definitionFields = definition.getFields();
    Object key = null;
    if (definition instanceof OPropertyIndexDefinition) {
      key = convert(conditions.get(definitionFields.get(0)), definition.getTypes()[0]);
    } else if (definition instanceof OCompositeIndexDefinition) {
      key = new OCompositeKey();
      for (int i = 0; i < nMatchingKeys; i++) {
        Object keyValue =
            convert(conditions.get(definitionFields.get(i)), definition.getTypes()[i]);
        ((OCompositeKey) key).addKey(keyValue);
      }
    }
    if (key != null) {
      if (conditions.size() == definitionFields.size()) {
        try (Stream<ORID> rids = index.getInternal().getRids(key)) {
          return rids.count();
        }
      } else if (index.supportsOrderedIterations()) {
        final Spliterator<ORawPair<Object, ORID>> spliterator;

        try (Stream<ORawPair<Object, ORID>> stream =
            index.getInternal().streamEntriesBetween(key, true, key, true, true)) {
          spliterator = stream.spliterator();
          return spliterator.estimateSize();
        }
      }
    }
    return Long.MAX_VALUE;
  }

  public Iterable fetchFromIndexes(OClass oClass, OCommandContext ctx) {

    List<OAndBlock> flattenedConditions = flatten();
    if (flattenedConditions == null || flattenedConditions.size() == 0) {
      return null;
    }
    Set<OIndex> indexes = oClass.getIndexes();
    List<OIndex> bestIndexes = new ArrayList<>();
    List<Map<String, Object>> indexConditions = new ArrayList<>();
    for (OAndBlock condition : flattenedConditions) {
      Map<String, Object> conditions = getEqualityOperations(condition, ctx);
      long conditionEstimation = Long.MAX_VALUE;
      OIndex bestIndex = null;
      Map<String, Object> bestCondition = null;

      for (OIndex index : indexes) {
        List<String> indexedFields = index.getDefinition().getFields();
        int nMatchingKeys = 0;
        for (String indexedField : indexedFields) {
          if (conditions.containsKey(indexedField)) {
            nMatchingKeys++;
          } else {
            break;
          }
        }
        if (nMatchingKeys > 0) {
          long newCount = estimateFromIndex(index, conditions, nMatchingKeys);
          if (newCount >= 0 && newCount <= conditionEstimation) {
            conditionEstimation = newCount;
            bestIndex = index;
            bestCondition = conditions;
          }
        }
      }
      if (bestIndex == null) {
        return null;
      }
      bestIndexes.add(bestIndex);
      indexConditions.add(bestCondition);
    }
    OMultiCollectionIterator result = new OMultiCollectionIterator();

    for (int i = 0; i < bestIndexes.size(); i++) {
      OIndex index = bestIndexes.get(i);
      Map<String, Object> condition = indexConditions.get(i);
      result.add(fetchFromIndex(index, indexConditions.get(i)));
    }
    return result;
  }

  private static Iterable fetchFromIndex(OIndex index, Map<String, Object> conditions) {
    OIndexDefinition definition = index.getDefinition();
    List<String> definitionFields = definition.getFields();
    Object key = null;
    if (definition instanceof OPropertyIndexDefinition) {
      key = convert(conditions.get(definitionFields.get(0)), definition.getTypes()[0]);
    } else if (definition instanceof OCompositeIndexDefinition) {
      key = new OCompositeKey();
      for (int i = 0; i < definitionFields.size(); i++) {
        String keyName = definitionFields.get(i);
        if (!conditions.containsKey(keyName)) {
          break;
        }
        Object keyValue = convert(conditions.get(keyName), definition.getTypes()[i]);
        ((OCompositeKey) key).addKey(conditions.get(keyName));
      }
    }
    if (key != null) {
      final Object iteratorKey = key;
      return () -> index.getInternal().getRids(iteratorKey).iterator();
    }
    return null;
  }

  private static Object convert(Object o, OType oType) {
    return OType.convert(o, oType.getDefaultJavaType());
  }

  private static Map<String, Object> getEqualityOperations(
      OAndBlock condition, OCommandContext ctx) {
    Map<String, Object> result = new HashMap<>();
    for (OBooleanExpression expression : condition.subBlocks) {
      if (expression instanceof OBinaryCondition) {
        OBinaryCondition b = (OBinaryCondition) expression;
        if (b.operator instanceof OEqualsCompareOperator) {
          if (b.left.isBaseIdentifier() && b.right.isEarlyCalculated(ctx)) {
            result.put(b.left.toString(), b.right.execute((OResult) null, ctx));
          }
        }
      }
    }
    return result;
  }

  public List<OAndBlock> flatten() {
    if (this.baseExpression == null) {
      return Collections.emptyList();
    }
    if (flattened == null) {
      flattened = this.baseExpression.flatten();
    }
    // TODO remove false conditions (contraddictions)
    return flattened;
  }

  public List<OBinaryCondition> getIndexedFunctionConditions(
      OClass iSchemaClass, ODatabaseDocumentInternal database) {
    if (baseExpression == null) {
      return null;
    }
    return this.baseExpression.getIndexedFunctionConditions(iSchemaClass, database);
  }

  public boolean needsAliases(Set<String> aliases) {
    return this.baseExpression.needsAliases(aliases);
  }

  public void setBaseExpression(OBooleanExpression baseExpression) {
    this.baseExpression = baseExpression;
  }

  public OWhereClause copy() {
    OWhereClause result = new OWhereClause(-1);
    result.baseExpression = baseExpression.copy();
    result.flattened =
        Optional.ofNullable(flattened)
            .map(
                oAndBlocks -> {
                  try (Stream<OAndBlock> stream = oAndBlocks.stream()) {
                    return stream.map(OAndBlock::copy).collect(Collectors.toList());
                  }
                })
            .orElse(null);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OWhereClause that = (OWhereClause) o;

    if (!Objects.equals(baseExpression, that.baseExpression)) return false;
    return Objects.equals(flattened, that.flattened);
  }

  @Override
  public int hashCode() {
    int result = Optional.ofNullable(baseExpression).map(Object::hashCode).orElse(0);
    result = 31 * result + (Optional.ofNullable(flattened).map(List::hashCode).orElse(0));
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (baseExpression != null) {
      baseExpression.extractSubQueries(collector);
    }
    flattened = null;
  }

  public boolean refersToParent() {
    return baseExpression != null && baseExpression.refersToParent();
  }

  public OBooleanExpression getBaseExpression() {
    return baseExpression;
  }

  public List<OAndBlock> getFlattened() {
    return flattened;
  }

  public void setFlattened(List<OAndBlock> flattened) {
    this.flattened = flattened;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    if (baseExpression != null) {
      result.setProperty("baseExpression", baseExpression.serialize());
    }
    if (flattened != null) {
      try (Stream<OAndBlock> stream = flattened.stream()) {
        result.setProperty(
            "flattened", stream.map(OBooleanExpression::serialize).collect(Collectors.toList()));
      }
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    if (fromResult.getProperty("baseExpression") != null) {
      baseExpression =
          OBooleanExpression.deserializeFromOResult(fromResult.getProperty("baseExpression"));
    }
    if (fromResult.getProperty("flattened") != null) {
      List<OResult> ser = fromResult.getProperty("flattened");
      flattened = new ArrayList<>();
      for (OResult r : ser) {
        OAndBlock block = new OAndBlock(-1);
        block.deserialize(r);
        flattened.add(block);
      }
    }
  }

  public boolean isCacheable() {
    return baseExpression.isCacheable();
  }

  public Optional<OIndexCandidate> findIndex(OIndexFinder info, OCommandContext ctx) {
    return this.baseExpression.findIndex(info, ctx);
  }
}
/* JavaCC - OriginalChecksum=e8015d01ce1ab2bc337062e9e3f2603e (do not edit this line) */
