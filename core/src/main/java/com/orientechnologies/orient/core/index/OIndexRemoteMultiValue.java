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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Set;

/**
 * Proxied index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexRemoteMultiValue extends OIndexRemote {
  private static final String QUERY_GET = "select expand( rid ) from index:`%s` where key = ?";

  public OIndexRemoteMultiValue(
      final String iName,
      final String iWrappedType,
      final String algorithm,
      final ORID iRid,
      final OIndexDefinition iIndexDefinition,
      final ODocument iConfiguration,
      final Set<String> clustersToIndex,
      String database) {
    super(
        iName,
        iWrappedType,
        algorithm,
        iRid,
        iIndexDefinition,
        iConfiguration,
        clustersToIndex,
        database);
  }

    @Override
  public boolean supportsOrderedIterations() {
    return false;
  }
}
