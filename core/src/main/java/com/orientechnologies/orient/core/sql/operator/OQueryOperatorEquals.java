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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import java.util.Arrays;
import java.util.Set;

/**
 * EQUALS operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorEquals() {
    super("=", 5, false);
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
  }

  public static boolean equals(final Object iLeft, final Object iRight, OType type) {
    if (type == null) {
      return equals(iLeft, iRight);
    }
    Object left = OType.convert(iLeft, type.getDefaultJavaType());
    Object right = OType.convert(iRight, type.getDefaultJavaType());
    return equals(left, right);
  }

  public static boolean equals(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null) return false;

    if (iLeft == iRight) {
      return true;
    }

    // RECORD & ORID
    /*from this is only legacy query engine */
    if (iLeft instanceof ORecord) return comparesValues(iRight, (ORecord) iLeft, true);
    else if (iRight instanceof ORecord) return comparesValues(iLeft, (ORecord) iRight, true);
    /*till this is only legacy query engine */
    else if (iRight instanceof OResult) return comparesValues(iLeft, (OResult) iRight, true);
    else if (iRight instanceof OResult) {
      return comparesValues(iLeft, (OResult) iRight, true);
    }

    // NUMBERS
    if (iLeft instanceof Number && iRight instanceof Number) {
      Number[] couple = OType.castComparableNumber((Number) iLeft, (Number) iRight);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      final Object right = OType.convert(iRight, iLeft.getClass());

      if (right == null) return false;
      if (iLeft instanceof byte[] && iRight instanceof byte[]) {
        return Arrays.equals((byte[]) iLeft, (byte[]) iRight);
      }
      return iLeft.equals(right);
    } catch (Exception ignore) {
      return false;
    }
  }

  protected static boolean comparesValues(
      final Object iValue, final ORecord iRecord, final boolean iConsiderIn) {
    // ORID && RECORD
    final ORID other = ((ORecord) iRecord).getIdentity();

    if (!other.isPersistent() && iRecord instanceof ODocument) {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = ((ODocument) iRecord).getPropertyNames();
      if (firstFieldName.size() > 0) {
        Object fieldValue = ((ODocument) iRecord).getProperty(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue, false)) {
              if (o != null && o.equals(iValue)) return true;
            }
          }

          return fieldValue.equals(iValue);
        }
      }
      return false;
    }
    return other.equals(iValue);
  }

  protected static boolean comparesValues(
      final Object iValue, final OResult iRecord, final boolean iConsiderIn) {
    if (iRecord.getIdentity().isPresent() && iRecord.getIdentity().get().isPersistent()) {
      return iRecord.getIdentity().get().equals(iValue);
    } else {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      Set<String> firstFieldName = iRecord.getPropertyNames();
      if (firstFieldName.size() == 1) {
        Object fieldValue = iRecord.getProperty(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue, false)) {
              if (o != null && o.equals(iValue)) return true;
            }
          }

          return fieldValue.equals(iValue);
        }
      }

      return false;
    }
  }

  @Override
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    return equals(iLeft, iRight);
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return serializer.getComparator().isEqual(iFirstField, iSecondField);
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }
}
