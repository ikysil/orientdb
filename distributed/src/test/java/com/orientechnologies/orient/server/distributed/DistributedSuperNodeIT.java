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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Assert;
import org.junit.Test;

/** Distributed TX test against "plocal" protocol. */
public class DistributedSuperNodeIT extends AbstractServerClusterGraphTest {
  @Test
  public void test() throws Exception {
    count = 200;
    init(3);
    prepare(false);
    execute();
  }

  @Override
  protected void setFactorySettings(ODatabasePool pool) {
    //
    // pool.setConnectionStrategy(OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST.toString());
  }

  @Override
  protected void onAfterExecution() {
    OServer server = serverInstance.get(0).getServerInstance();
    try (ODatabaseDocument graph = server.openDatabase(getDatabaseName(), "admin", "admin")) {

      ODocument rootDoc = graph.load(rootVertexId);
      final OVertex root = rootDoc.asVertex().get();

      Assert.assertEquals(
          ((OMultiCollectionIterator) root.getEdges(ODirection.OUT)).size(),
          count * serverInstance.size() * writerCount);
    }
    super.onAfterExecution();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-graph";
  }
}
