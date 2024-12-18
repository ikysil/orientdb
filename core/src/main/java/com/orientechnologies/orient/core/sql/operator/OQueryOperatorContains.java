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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import java.util.Iterator;
import java.util.Map;

/**
 * CONTAINS operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorContains extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContains() {
    super("CONTAINS", 5, false);
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

    if (iLeft instanceof Iterable<?>) {

      final Iterable<Object> iterable = (Iterable<Object>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (final Object o : iterable) {
          final OIdentifiable id;
          if (o instanceof OIdentifiable) id = (OIdentifiable) o;
          else if (o instanceof Map<?, ?>) {
            final Iterator<Object> iter = ((Map<?, Object>) o).values().iterator();
            final Object v = iter.hasNext() ? iter.next() : null;
            if (v instanceof OIdentifiable) id = (OIdentifiable) v;
            else
              // TRANSFORM THE ENTIRE MAP IN A DOCUMENT. PROBABLY HAS BEEN IMPORTED FROM JSON
              id = new ODocument((Map) o);

          } else if (o instanceof Iterable<?>) {
            final Iterator<OIdentifiable> iter = ((Iterable<OIdentifiable>) o).iterator();
            id = iter.hasNext() ? iter.next() : null;
          } else continue;

          if ((Boolean) condition.evaluate(id, null, iContext) == Boolean.TRUE) return true;
        }
      } else {
        // CHECK AGAINST A SINGLE VALUE
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
        for (final Object o : iterable) {
          if (OQueryOperatorEquals.equals(iRight, o, type)) return true;
        }
      }
    } else if (iRight instanceof Iterable<?>) {

      // CHECK AGAINST A CONDITION
      final Iterable<OIdentifiable> iterable = (Iterable<OIdentifiable>) iRight;

      if (condition != null) {
        for (final OIdentifiable o : iterable) {
          if ((Boolean) condition.evaluate(o, null, iContext) == Boolean.TRUE) return true;
        }
      } else {
        // CHECK AGAINST A SINGLE VALUE
        for (final Object o : iterable) {
          if (OQueryOperatorEquals.equals(iLeft, o)) return true;
        }
      }
    }
    return false;
  }
}
