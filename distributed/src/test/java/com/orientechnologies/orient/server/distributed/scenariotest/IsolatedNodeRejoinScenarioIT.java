/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.server.distributed.scenariotest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (quorum=2) -
 * server3 is isolated (simulated by shutdown) - 5 threads on both server1 and server2 write 100
 * records - server3 joins the cluster - server3 receive the delta from the cluster - check
 * consistency
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class IsolatedNodeRejoinScenarioIT extends AbstractScenarioTest {

  @Test
  @Ignore
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server1 and server2
    executeTestsOnServers = new ArrayList<ServerRun>();
    executeTestsOnServers.add(serverInstance.get(0));
    executeTestsOnServers.add(serverInstance.get(1));

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
     * Test with quorum = 1
     */

    banner("Test with quorum = 2");

    System.out.print("\nChanging configuration (writeQuorum=2, autoDeploy=false)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    ODistributedPlugin manager =
        (ODistributedPlugin) server.getServerInstance().getDistributedManager();
    OModifiableDistributedConfiguration databaseConfiguration =
        manager.getDatabaseConfiguration(getDatabaseName()).modify();
    cfg = databaseConfiguration.getDocument();
    cfg.field("writeQuorum", 2);
    cfg.field("autoDeploy", true);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager.updateCachedDatabaseConfiguration(getDatabaseName(), databaseConfiguration);
    System.out.println("\nConfiguration updated.");

    // isolating server3
    System.out.println("Network fault on server3.\n");
    simulateServerFault(serverInstance.get(2), "net-fault");
    assertFalse(serverInstance.get(2).isActive());

    // execute writes on server1 and server2
    executeMultipleWrites(super.executeTestsOnServers, "plocal");

    // server3 joins the cluster
    System.out.println("Restart server3.\n");
    try {
      serverInstance.get(2).startServer(getDistributedServerConfiguration(server));
    } catch (Exception e) {
      fail();
    }

    // waiting for propagation
    waitForMultipleInsertsInClassPropagation(1000L, "Person", 5000L);

    // check consistency
    super.checkWritesAboveCluster(serverInstance, executeTestsOnServers);
  }

  @Override
  protected void onBeforeChecks() throws InterruptedException {
    // // WAIT UNTIL THE END
    waitFor(
        0,
        new OCallable<Boolean, ODatabaseDocument>() {
          @Override
          public Boolean call(ODatabaseDocument db) {
            final boolean ok = db.countClass("Person") >= 1000L;
            if (!ok)
              System.out.println(
                  "FOUND "
                      + db.countClass("Person")
                      + " people on server 0 instead of expected "
                      + 1000L);
            return ok;
          }
        },
        10000);

    waitFor(
        1,
        new OCallable<Boolean, ODatabaseDocument>() {
          @Override
          public Boolean call(ODatabaseDocument db) {
            final boolean ok = db.countClass("Person") >= 1000L;
            if (!ok)
              System.out.println(
                  "FOUND "
                      + db.countClass("Person")
                      + " people on server 1 instead of expected "
                      + 1000L);
            return ok;
          }
        },
        10000);

    Thread.sleep(2000);
  }

  @Override
  protected ODocument retrieveRecord(ServerRun serverRun, String uniqueId) {
    ODatabaseDocument dbServer = getDatabase(serverRun);

    List<OResult> result =
        dbServer.query("select from Hero where id = '" + uniqueId + "'").stream().toList();
    try {
      if (result.size() == 0) fail("No record found with id = '" + uniqueId + "'!");
      else if (result.size() > 1)
        fail(result.size() + " records found with id = '" + uniqueId + "'!");
    } finally {
      dbServer.close();
    }
    return (ODocument) result.get(0).getElement().get();
  }

  @Override
  public String getDatabaseName() {
    return "distributed-isolated-node-rejoin";
  }
}
