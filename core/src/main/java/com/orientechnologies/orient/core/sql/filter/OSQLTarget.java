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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.util.Map;

/**
 * Target parser.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLTarget extends OBaseParser {
  protected final OCommandContext context;
  protected String targetVariable;
  protected String targetQuery;
  protected Iterable<? extends OIdentifiable> targetRecords;
  protected Map<String, String> targetClusters;
  protected Map<String, String> targetClasses;

  protected String targetIndex;

  protected String targetIndexValues;
  protected boolean targetIndexValuesAsc;

  public OSQLTarget(final String iText, final OCommandContext iContext) {
    super();
    context = iContext;
    parserText = iText;
    parserTextUpperCase = OSQLPredicate.upperCase(iText);
  }

  public Map<String, String> getTargetClusters() {
    return targetClusters;
  }

  public Map<String, String> getTargetClasses() {
    return targetClasses;
  }

  public Iterable<? extends OIdentifiable> getTargetRecords() {
    return targetRecords;
  }

  public String getTargetQuery() {
    return targetQuery;
  }

  public String getTargetIndex() {
    return targetIndex;
  }

  public String getTargetIndexValues() {
    return targetIndexValues;
  }

  public boolean isTargetIndexValuesAsc() {
    return targetIndexValuesAsc;
  }

  @Override
  public String toString() {
    if (targetClasses != null) return "class " + targetClasses.keySet();
    else if (targetClusters != null) return "cluster " + targetClusters.keySet();
    if (targetIndex != null) return "index " + targetIndex;
    if (targetRecords != null) return "records from " + targetRecords.getClass().getSimpleName();
    if (targetVariable != null) return "variable " + targetVariable;
    return "?";
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandSQLParsingException(
        iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }
}
