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
package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import java.util.List;

/**
 * Wraps function managing the binding of parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionRuntime extends OSQLFilterItemAbstract {

  public OSQLFunction function;
  public Object[] configuredParameters;
  public Object[] runtimeParameters;

  public OSQLFunctionRuntime(final OBaseParser iQueryToParse, final String iText) {
    super(iQueryToParse, iText);
  }

  public OSQLFunctionRuntime(final OSQLFunction iFunction) {
    function = iFunction;
  }

  public boolean aggregateResults() {
    return function.aggregateResults();
  }

  public boolean filterResult() {
    return function.filterResult();
  }

  /**
   * Execute a function.
   *
   * @param iCurrentRecord Current record
   * @param iCurrentResult TODO
   * @param iContext
   * @return
   */
  public Object execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final OCommandContext iContext) {
    // RESOLVE VALUES USING THE CURRENT RECORD
    for (int i = 0; i < configuredParameters.length; ++i) {
      runtimeParameters[i] = configuredParameters[i];

      if (configuredParameters[i] instanceof OSQLFunctionRuntime)
        runtimeParameters[i] =
            ((OSQLFunctionRuntime) configuredParameters[i])
                .execute(iThis, iCurrentRecord, iCurrentResult, iContext);
      else if (configuredParameters[i] instanceof String) {
        if (configuredParameters[i].toString().startsWith("\"")
            || configuredParameters[i].toString().startsWith("'"))
          runtimeParameters[i] = OIOUtils.getStringContent(configuredParameters[i]);
      }
    }

    if (function.getMaxParams() == -1 || function.getMaxParams() > 0) {
      if (runtimeParameters.length < function.getMinParams()
          || (function.getMaxParams() > -1 && runtimeParameters.length > function.getMaxParams())) {
        String params;
        if (function.getMinParams() == function.getMaxParams()) {
          params = "" + function.getMinParams();
        } else {
          params = function.getMinParams() + "-" + function.getMaxParams();
        }
        throw new OCommandExecutionException(
            "Syntax error: function '"
                + function.getName()
                + "' needs "
                + params
                + " argument(s) while has been received "
                + runtimeParameters.length);
      }
    }

    final Object functionResult =
        function.execute(iThis, iCurrentRecord, iCurrentResult, runtimeParameters, iContext);

    if (functionResult instanceof OAutoConvertToRecord)
      // FORCE AVOIDING TO CONVERT IN RECORD
      ((OAutoConvertToRecord) functionResult).setAutoConvertToRecord(false);

    return transformValue(iCurrentRecord, iContext, functionResult);
  }

  public Object getResult() {
    return null;
  }

  public void setResult(final Object iValue) {
    function.setResult(iValue);
  }

  @Override
  public Object getValue(
      final OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContext) {
    final ODocument current = iRecord != null ? (ODocument) iRecord.getRecord() : null;
    return null;
  }

  @Override
  public String getRoot() {
    return function.getName();
  }

  public OSQLFunctionRuntime setParameters(final Object[] iParameters, final boolean iEvaluate) {
    this.configuredParameters = new Object[iParameters.length];
    for (int i = 0; i < iParameters.length; ++i) {
      this.configuredParameters[i] = iParameters[i];
    }

    function.config(configuredParameters);

    // COPY STATIC VALUES
    this.runtimeParameters = new Object[configuredParameters.length];
    for (int i = 0; i < configuredParameters.length; ++i) {
      runtimeParameters[i] = configuredParameters[i];
    }

    return this;
  }

  public OSQLFunction getFunction() {
    return function;
  }

  public Object[] getConfiguredParameters() {
    return configuredParameters;
  }

  public Object[] getRuntimeParameters() {
    return runtimeParameters;
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iText) {
    final int beginParenthesis = iText.indexOf('(');

    // SEARCH FOR THE FUNCTION
    final String funcName = iText.substring(0, beginParenthesis);

    final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

    function = OSQLEngine.getInstance().getFunction(funcName);
    if (function == null)
      throw new OCommandSQLParsingException("Unknown function " + funcName + "()");

    // PARSE PARAMETERS
    this.configuredParameters = new Object[funcParamsText.size()];
    for (int i = 0; i < funcParamsText.size(); ++i)
      this.configuredParameters[i] = funcParamsText.get(i);

    setParameters(configuredParameters, true);
  }
}
