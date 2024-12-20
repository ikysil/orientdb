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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Parses text in SQL format and build a tree of conditions.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLPredicate extends OBaseParser implements OCommandPredicate {
  protected Set<OProperty> properties = new HashSet<OProperty>();
  protected List<String> recordTransformed;
  protected int braces;
  protected OCommandContext context;

  public OSQLPredicate() {}

  protected void throwSyntaxErrorException(final String iText) {
    final String syntax = getSyntax();
    if (syntax.equals("?"))
      throw new OCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());

    throw new OCommandSQLParsingException(
        iText + ". Use " + syntax, parserText, parserGetPreviousPosition());
  }

  public static String upperCase(String text) {
    StringBuilder result = new StringBuilder(text.length());
    for (char c : text.toCharArray()) {
      String upper = ("" + c).toUpperCase(Locale.ENGLISH);
      if (upper.length() > 1) {
        result.append(c);
      } else {
        result.append(upper);
      }
    }
    return result.toString();
  }

  protected boolean checkForEnd(final String iWord) {
    if (iWord != null
        && (iWord.equals("order")
            || iWord.equals("limit")
            || iWord.equals("skip")
            || iWord.equals("offset"))) {
      parserMoveCurrentPosition(iWord.length() * -1);
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "Unparsed: " + parserText;
  }
}
