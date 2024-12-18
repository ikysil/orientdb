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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * IS operator. Different by EQUALS since works also for null. Example "IS null"
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorIs extends OQueryOperatorEquality {

  public OQueryOperatorIs() {
    super("IS", 5, false);
  }

  @Override
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      Object iRight,
      OCommandContext iContext) {
    if (iCondition.getLeft() instanceof OSQLFilterItemField) {
      if (OSQLHelper.DEFINED.equals(iCondition.getRight()))
        return evaluateDefined(iRecord, "" + iCondition.getLeft());

      if (iCondition.getRight() instanceof OSQLFilterItemField
          && "not defined".equalsIgnoreCase("" + iCondition.getRight()))
        return !evaluateDefined(iRecord, "" + iCondition.getLeft());
    }

    if (OSQLHelper.NOT_NULL.equals(iRight)) return iLeft != null;
    else if (OSQLHelper.NOT_NULL.equals(iLeft)) return iRight != null;
    else if (OSQLHelper.DEFINED.equals(iLeft)) return evaluateDefined(iRecord, (String) iRight);
    else if (OSQLHelper.DEFINED.equals(iRight)) return evaluateDefined(iRecord, (String) iLeft);
    else return iLeft == iRight;
  }

  protected boolean evaluateDefined(final OIdentifiable iRecord, final String iFieldName) {
    if (iRecord instanceof ODocument) {
      return ((ODocument) iRecord).containsField(iFieldName);
    }
    return false;
  }
}
