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
package com.orientechnologies.orient.core.sql.method;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import java.util.List;

/**
 * Wraps function managing the binding of parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodRuntime extends OSQLFilterItemAbstract
    implements Comparable<OSQLMethodRuntime> {

  public OSQLMethod method;
  public Object[] configuredParameters;
  public Object[] runtimeParameters;

  public OSQLMethodRuntime(final OSQLMethod iFunction) {
    method = iFunction;
  }

  /**
   * Execute a method.
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
    return null;
  }

  @Override
  public Object getValue(
      final OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContext) {
    return null;
  }

  @Override
  public String getRoot() {
    return method.getName();
  }

  @Override
  protected void setRoot(final OBaseParser iQueryToParse, final String iText) {
    final int beginParenthesis = iText.indexOf('(');

    // SEARCH FOR THE FUNCTION
    final String funcName = iText.substring(0, beginParenthesis);

    final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

    method = OSQLEngine.getMethod(funcName);
    if (method == null) throw new OCommandSQLParsingException("Unknown method " + funcName + "()");

    // PARSE PARAMETERS
    this.configuredParameters = new Object[funcParamsText.size()];
    for (int i = 0; i < funcParamsText.size(); ++i)
      this.configuredParameters[i] = funcParamsText.get(i);

    setParameters(configuredParameters, true);
  }

  public OSQLMethodRuntime setParameters(final Object[] iParameters, final boolean iEvaluate) {
    if (iParameters != null) {
      this.configuredParameters = new Object[iParameters.length];
      for (int i = 0; i < iParameters.length; ++i) {
        this.configuredParameters[i] = iParameters[i];

        if (iParameters[i] != null) {
          if (iParameters[i] instanceof String && !iParameters[i].toString().startsWith("[")) {
            final Object v = OSQLHelper.parseValue(null, iParameters[i].toString(), null);
            if (v == OSQLHelper.VALUE_NOT_PARSED
                || (v != null
                    && OMultiValue.isMultiValue(v)
                    && OMultiValue.getFirstValue(v) == OSQLHelper.VALUE_NOT_PARSED)) continue;

            configuredParameters[i] = v;
          }
        } else this.configuredParameters[i] = null;
      }

      // COPY STATIC VALUES
      this.runtimeParameters = new Object[configuredParameters.length];
      for (int i = 0; i < configuredParameters.length; ++i) {
        runtimeParameters[i] = configuredParameters[i];
      }
    }

    return this;
  }

  public OSQLMethod getMethod() {
    return method;
  }

  public Object[] getConfiguredParameters() {
    return configuredParameters;
  }

  public Object[] getRuntimeParameters() {
    return runtimeParameters;
  }

  @Override
  public int compareTo(final OSQLMethodRuntime o) {
    return method.compareTo(o.getMethod());
  }
}
