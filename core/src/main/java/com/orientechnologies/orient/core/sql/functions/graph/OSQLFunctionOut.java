package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 03/01/17. */
public class OSQLFunctionOut extends OSQLFunctionMoveFiltered {
  public static final String NAME = "out";

  public OSQLFunctionOut() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final ODatabaseSession graph, final OResult iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, ODirection.OUT, iLabels);
  }

  protected Object move(
      final ODatabaseSession graph,
      final OResult rec,
      final String[] iLabels,
      Iterable<OIdentifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, rec, ODirection.OUT, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    Object edges = v2e(graph, rec, ODirection.OUT, iLabels);
    if (edges instanceof OSizeable) {
      int size = ((OSizeable) edges).size();
      if (size > supernodeThreshold) {
        Object result = fetchFromIndex(graph, rec, iPossibleResults, iLabels);
        if (result != null) {
          return result;
        }
      }
    }

    return v2v(graph, rec, ODirection.OUT, iLabels);
  }

  private Object fetchFromIndex(
      ODatabaseSession graph, OResult iFrom, Iterable<OIdentifiable> iTo, String[] iEdgeTypes) {
    String edgeClassName = null;
    if (iEdgeTypes == null) {
      edgeClassName = "E";
    } else if (iEdgeTypes.length == 1) {
      edgeClassName = iEdgeTypes[0];
    } else {
      return null;
    }
    OClass edgeClass =
        ((ODatabaseDocumentInternal) graph)
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    Set<OIndex> indexes = edgeClass.getInvolvedIndexes("out", "in");
    if (indexes == null || indexes.size() == 0) {
      return null;
    }
    OIndex index = indexes.iterator().next();

    OMultiCollectionIterator<OVertex> result = new OMultiCollectionIterator<OVertex>();
    for (OIdentifiable to : iTo) {
      final OCompositeKey key = new OCompositeKey(iFrom.getIdentity().get(), to);
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        result.add(
            stream
                .map((rid) -> ((ODocument) rid.getRecord()).rawField("in"))
                .collect(Collectors.toSet()));
      }
    }

    return result;
  }
}
