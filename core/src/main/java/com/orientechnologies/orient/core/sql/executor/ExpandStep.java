package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsResultSet;
import java.util.Iterator;

/**
 * Expands a result-set. The pre-requisite is that the input element contains only one field (no
 * matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {

  private long cost = 0;

  public ExpandStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = getPrev().get().syncPull(ctx);

    OExecutionStream result =
        new OSubResultsResultSet(
            new Iterator<OExecutionStream>() {
              private OExecutionStream next;

              @Override
              public boolean hasNext() {
                fetchNext();
                return next != null;
              }

              private void fetchNext() {
                if (next == null) {
                  next = nextSequence(ctx, resultSet);
                }
              }

              @Override
              public OExecutionStream next() {
                if (!hasNext()) {
                  throw new IllegalStateException();
                }
                OExecutionStream n = next;
                this.next = null;
                return n;
              }
            });
    return result;
  }

  public OExecutionStream nextSequence(OCommandContext ctx, OExecutionStream resultSet) {
    while (resultSet.hasNext(ctx)) {
      OResult nextAggregateItem = resultSet.next(ctx);
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        if (nextAggregateItem.getPropertyNames().size() == 0) {
          continue;
        }
        if (nextAggregateItem.getPropertyNames().size() > 1) {
          throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
        }

        String propName = nextAggregateItem.getPropertyNames().iterator().next();
        Object projValue = nextAggregateItem.getProperty(propName);
        if (projValue == null) {
          continue;
        }
        if (projValue instanceof OIdentifiable) {
          ORecord rec = ((OIdentifiable) projValue).getRecord();
          if (rec == null) {
            continue;
          }
          OResultInternal res = new OResultInternal(rec);

          return OExecutionStream.singleton((OResult) res);
        } else if (projValue instanceof OResult) {
          return OExecutionStream.singleton((OResult) projValue);
        } else if (projValue instanceof Iterator) {
          return OExecutionStream.iterator((Iterator) projValue);
        } else if (projValue instanceof Iterable) {
          return OExecutionStream.iterator(((Iterable) projValue).iterator());
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXPAND";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
