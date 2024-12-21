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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * SQL Helper class
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLHelper {
  public static final String NAME = "sql";

  public static final String VALUE_NOT_PARSED = "_NOT_PARSED_";
  public static final String NOT_NULL = "_NOT_NULL_";
  public static final String DEFINED = "_DEFINED_";

  public static Object parseDefaultValue(ODocument iRecord, final String iWord) {
    return OSQLEngine.eval(iWord, iRecord, new OBasicCommandContext());
  }

  public static Object getValue(
      final Object iObject, final ORecord iRecord, final OCommandContext iContext) {
    if (iObject == null) return null;

    if (iObject instanceof String) {
      final String s = ((String) iObject).trim();
      if (iRecord != null & !s.isEmpty()
          && !OIOUtils.isStringContent(iObject)
          && !Character.isDigit(s.charAt(0))
          && iRecord instanceof ODocument)
        // INTERPRETS IT
        return ((ODocument) iRecord).eval(s, iContext);
    }

    return iObject;
  }
}
