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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.tx.OTransaction;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "dictionary")
public class TransactionIsolationTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public TransactionIsolationTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testIsolationRepeatableRead() throws IOException {

    ODatabaseSession db1 = openSession("admin", "admin");

    ODocument record1 = new ODocument();
    record1
        .field("name", "This is the first version")
        .save(db1.getClusterNameById(db1.getDefaultClusterId()));

    db1.begin();
    try {
      db1.getTransaction().setIsolationLevel(OTransaction.ISOLATION_LEVEL.REPEATABLE_READ);

      // RE-READ THE RECORD
      record1.getIdentity().getRecord();

      // CHANGE THE RECORD FROM DB2
      ODatabaseSession db2 = openSession("admin", "admin");

      ODocument record2 = db2.load(record1.getIdentity());
      record2.field("name", "This is the second version").save();

      db2.close();

      db1.activateOnCurrentThread();
      db1.reload(record1, null, true);

      Assert.assertEquals(record1.field("name"), "This is the first version");
    } catch (IllegalArgumentException e) {
      if (!url.startsWith("remote:"))
        // NOT SUPPORTED IN REMOTE MODE
        Assert.assertFalse(true);
    }

    db1.close();
  }

  @Test
  public void testIsolationReadCommitted() throws IOException {
    ODatabaseSession db1 = openSession("admin", "admin");

    ODocument record1 = new ODocument();
    record1
        .field("name", "This is the first version")
        .save(db1.getClusterNameById(db1.getDefaultClusterId()));

    db1.begin();
    db1.getTransaction().setIsolationLevel(OTransaction.ISOLATION_LEVEL.READ_COMMITTED);

    // RE-READ THE RECORD
    record1.getIdentity().getRecord();

    // CHANGE THE RECORD FROM DB2
    ODatabaseSession db2 = openSession("admin", "admin");

    ODocument record2 = db2.load(record1.getIdentity());
    record2.field("name", "This is the second version").save();

    db1.activateOnCurrentThread();
    db1.reload(record1, null, true);

    Assert.assertEquals(record1.field("name"), "This is the second version");

    db1.close();

    db2.activateOnCurrentThread();
    db2.close();
  }

  @Test
  public void testIsolationRepeatableReadScript() throws ExecutionException, InterruptedException {
    ODatabaseSession db1 = openSession("admin", "admin");

    final ODocument record1 = new ODocument();
    record1
        .field("name", "This is the first version")
        .save(db1.getClusterNameById(db1.getDefaultClusterId()));

    Future<List<OResult>> txFuture =
        ((ODatabaseDocumentInternal) db1)
            .getSharedContext()
            .getOrientDB()
            .execute(
                new Callable<List<OResult>>() {
                  @Override
                  public List<OResult> call() throws Exception {
                    try {
                      String cmd = "";
                      cmd += "begin isolation REPEATABLE_READ;";
                      cmd += "let r1 = select from " + record1.getIdentity() + ";";
                      cmd += "sleep 2000;";
                      cmd += "let r2 = select from " + record1.getIdentity() + ";";
                      cmd += "commit;";
                      cmd += "return $r2;";

                      db1.activateOnCurrentThread();
                      return db1.execute("sql", cmd).stream().toList();
                    } finally {
                      ODatabaseRecordThreadLocal.instance().remove();
                    }
                  }
                });

    Thread.sleep(500);

    // CHANGE THE RECORD FROM DB2
    ODatabaseSession db2 = openSession("admin", "admin");

    ODocument record2 = db2.load(record1.getIdentity());
    record2.field("name", "This is the second version").save();

    List<OResult> txRecord = txFuture.get();

    Assert.assertNotNull(txRecord);
    Assert.assertEquals(txRecord.size(), 1);
    Assert.assertEquals(txRecord.get(0).getProperty("name"), "This is the first version");

    db1.activateOnCurrentThread();
    db1.close();

    db2.activateOnCurrentThread();
    db2.close();
  }

  @Test
  public void testIsolationReadCommittedScript() throws ExecutionException, InterruptedException {
    ODatabaseSession db1 = openSession("admin", "admin");

    final ODocument record1 = new ODocument();
    record1
        .field("name", "This is the first version")
        .save(db1.getClusterNameById(db1.getDefaultClusterId()));

    Future<List<OResult>> txFuture =
        ((ODatabaseDocumentInternal) db1)
            .getSharedContext()
            .getOrientDB()
            .execute(
                new Callable<List<OResult>>() {
                  @Override
                  public List<OResult> call() throws Exception {
                    try {
                      String cmd = "";
                      cmd += "begin isolation READ_COMMITTED;";
                      cmd += "let r1 = select from " + record1.getIdentity() + ";";
                      cmd += "sleep 2000;";
                      cmd += "let r2 = select from " + record1.getIdentity() + " nocache;";
                      cmd += "commit;";
                      cmd += "return $r2;";

                      db1.activateOnCurrentThread();
                      return db1.execute("sql", cmd).stream().toList();
                    } finally {
                      ODatabaseRecordThreadLocal.instance().remove();
                    }
                  }
                });

    Thread.sleep(500);

    // CHANGE THE RECORD FROM DB2
    ODatabaseSession db2 = openSession("admin", "admin");

    ODocument record2 = db2.load(record1.getIdentity());
    record2.field("name", "This is the second version");
    db2.save(record2);

    List<OResult> txRecord = txFuture.get();

    Assert.assertNotNull(txRecord);
    Assert.assertEquals(txRecord.size(), 1);
    Assert.assertEquals(txRecord.get(0).getProperty("name"), "This is the first version");

    db2.close();

    db1.activateOnCurrentThread();
    db1.close();
  }
}
