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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import java.util.Iterator;

/**
 * BETWEEN operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorBetween extends OQueryOperatorEqualityNotNulls {
  private boolean leftInclusive = true;
  private boolean rightInclusive = true;

  public OQueryOperatorBetween() {
    super("BETWEEN", 5, false, 3);
  }

  public boolean isLeftInclusive() {
    return leftInclusive;
  }

  public void setLeftInclusive(boolean leftInclusive) {
    this.leftInclusive = leftInclusive;
  }

  public boolean isRightInclusive() {
    return rightInclusive;
  }

  public void setRightInclusive(boolean rightInclusive) {
    this.rightInclusive = rightInclusive;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition condition,
      final Object left,
      final Object right,
      OCommandContext iContext) {
    validate(right);

    final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(right, false);

    Object right1 = valueIterator.next();
    valueIterator.next();
    Object right2 = valueIterator.next();
    final Object right1c = OType.convert(right1, left.getClass());
    if (right1c == null) return false;

    final Object right2c = OType.convert(right2, left.getClass());
    if (right2c == null) return false;

    final int leftResult;
    if (left instanceof Number && right1 instanceof Number) {
      Number[] conv = OType.castComparableNumber((Number) left, (Number) right1);
      leftResult = ((Comparable) conv[0]).compareTo(conv[1]);
    } else {
      leftResult = ((Comparable<Object>) left).compareTo(right1c);
    }
    final int rightResult;
    if (left instanceof Number && right2 instanceof Number) {
      Number[] conv = OType.castComparableNumber((Number) left, (Number) right2);
      rightResult = ((Comparable) conv[0]).compareTo(conv[1]);
    } else {
      rightResult = ((Comparable<Object>) left).compareTo(right2c);
    }

    return (leftInclusive ? leftResult >= 0 : leftResult > 0)
        && (rightInclusive ? rightResult <= 0 : rightResult < 0);
  }

  private void validate(Object iRight) {
    if (!OMultiValue.isMultiValue(iRight.getClass())) {
      throw new IllegalArgumentException(
          "Found '" + iRight + "' while was expected: " + getSyntax());
    }

    if (OMultiValue.getSize(iRight) != 3)
      throw new IllegalArgumentException(
          "Found '" + OMultiValue.toString(iRight) + "' while was expected: " + getSyntax());
  }

  @Override
  public String getSyntax() {
    return "<left> " + keyword + " <minRange> AND <maxRange>";
  }
}
