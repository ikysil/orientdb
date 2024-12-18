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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import java.util.Map;

/**
 * CONTAINS KEY operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorContainsValue extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContainsValue() {
    super("CONTAINSVALUE", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    final OSQLFilterCondition condition;
    if (iCondition.getLeft() instanceof OSQLFilterCondition)
      condition = (OSQLFilterCondition) iCondition.getLeft();
    else if (iCondition.getRight() instanceof OSQLFilterCondition)
      condition = (OSQLFilterCondition) iCondition.getRight();
    else condition = null;

    OType type = null;
    if (iCondition.getLeft() instanceof OSQLFilterItemField
        && ((OSQLFilterItemField) iCondition.getLeft()).isFieldChain()
        && ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemCount() == 1) {
      String fieldName =
          ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemName(0);
      if (fieldName != null) {
        Object record = iRecord.getRecord();
        if (record instanceof ODocument) {
          OProperty property =
              ODocumentInternal.getImmutableSchemaClass(((ODocument) record))
                  .getProperty(fieldName);
          if (property != null && property.getType().isMultiValue()) {
            type = property.getLinkedType();
          }
        }
      }
    }

    Object right = iRight;
    if (type != null) {
      right = OType.convert(iRight, type.getDefaultJavaType());
    }

    if (iLeft instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext)) return true;
        }
      } else {
        for (Object val : map.values()) {
          Object convertedRight = iRight;
          if (val instanceof ODocument && iRight instanceof Map) {
            val = ((ODocument) val).toMap();
          }
          if (val instanceof Map && iRight instanceof ODocument) {
            convertedRight = ((ODocument) iRight).toMap();
          }
          if (OQueryOperatorEquals.equals(val, convertedRight)) {
            return true;
          }
        }
        return false;
      }

    } else if (iRight instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iRight;

      if (condition != null)
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext)) return true;
          else return map.containsValue(iLeft);
        }
    }
    return false;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object loadIfNeed(Object o) {
    final ORecord record = (ORecord) o;
    if (record.getRecord().getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        o = record.<ORecord>load();
      } catch (ORecordNotFoundException e) {
        throw OException.wrapException(
            new ODatabaseException("Error during loading record with id : " + record.getIdentity()),
            e);
      }
    }
    return o;
  }
}
