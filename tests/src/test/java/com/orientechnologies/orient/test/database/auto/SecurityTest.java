/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "security")
public class SecurityTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SecurityTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    database.close();
  }

  public void testWrongPassword() throws IOException {
    try {
      reopendb("reader", "swdsds");
    } catch (OException e) {
      Assert.assertTrue(
          e instanceof OSecurityAccessException
              || e.getCause() != null
                  && e.getCause()
                          .toString()
                          .indexOf(
                              "com.orientechnologies.orient.core.exception.OSecurityAccessException")
                      > -1);
    }
  }

  public void testSecurityAccessWriter() throws IOException {
    reopendb("writer", "writer");

    try {
      database.save(new ODocument(), "internal");
      Assert.assertTrue(false);
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testSecurityAccessReader() throws IOException {
    reopendb("reader", "reader");

    try {
      new ODocument("Profile")
          .fields(
              "nick",
              "error",
              "password",
              "I don't know",
              "lastAccessOn",
              new Date(),
              "registeredOn",
              new Date())
          .save();
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testEncryptPassword() throws IOException {
    reopendb("admin", "admin");

    Long updated =
        database
            .command("update ouser set password = 'test' where name = 'reader'")
            .next()
            .getProperty("count");
    Assert.assertEquals(updated.intValue(), 1);

    OResultSet result = database.query("select from ouser where name = 'reader'");
    Assert.assertFalse(result.next().getProperty("password").equals("test"));

    // RESET OLD PASSWORD
    updated =
        database
            .command("update ouser set password = 'reader' where name = 'reader'")
            .next()
            .getProperty("count");
    Assert.assertEquals(updated.intValue(), 1);

    result = database.query("select from ouser where name = 'reader'");
    Assert.assertFalse(result.next().getProperty("password").equals("reader"));

    database.close();
  }

  public void testParentRole() {
    reopendb("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();
    ORole writer = security.getRole("writer");

    ORole writerChild =
        security.createRole("writerChild", writer, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
    writerChild.save();

    try {
      ORole writerGrandChild =
          security.createRole(
              "writerGrandChild", writerChild, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
      writerGrandChild.save();

      try {
        OUser child = security.createUser("writerChild", "writerChild", writerGrandChild);
        child.save();

        try {
          Assert.assertTrue(child.hasRole("writer", true));
          Assert.assertFalse(child.hasRole("wrter", true));

          database.close();
          if (!database.isRemote()) {
            reopendb("writerChild", "writerChild");

            OSecurityUser user = database.getUser();
            Assert.assertTrue(user.hasRole("writer", true));
            Assert.assertFalse(user.hasRole("wrter", true));

            database.close();
          }
          reopendb("admin", "admin");
          security = database.getMetadata().getSecurity();
        } finally {
          security.dropUser("writerChild");
        }
      } finally {
        security.dropRole("writerGrandChild");
      }
    } finally {
      security.dropRole("writerChild");
    }
  }

  @Test
  public void testQuotedUserName() {
    reopendb("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    ORole adminRole = security.getRole("admin");
    OUser newUser = security.createUser("user'quoted", "foobar", adminRole);

    database.close();

    reopendb("user'quoted", "foobar");
    database.close();

    reopendb("admin", "admin");
    security = database.getMetadata().getSecurity();
    OUser user = security.getUser("user'quoted");
    Assert.assertNotNull(user);
    security.dropUser(user.getName());

    database.close();

    try {
      reopendb("user'quoted", "foobar");
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testUserNoRole() {
    reopendb("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    OUser newUser = security.createUser("noRole", "noRole", (String[]) null);

    database.close();

    try {
      reopendb("noRole", "noRole");
      Assert.fail();
    } catch (OSecurityAccessException e) {
      reopendb("admin", "admin");
      security = database.getMetadata().getSecurity();
      security.dropUser("noRole");
    }
  }

  @Test
  public void testAdminCanSeeSystemClusters() {
    reopendb("admin", "admin");

    List<OResult> result =
        database.command("select from ouser").stream().collect(Collectors.toList());
    Assert.assertFalse(result.isEmpty());

    Assert.assertTrue(database.browseClass("OUser").hasNext());

    Assert.assertTrue(database.browseCluster("OUser").hasNext());
  }

  @Test
  public void testOnlyAdminCanSeeSystemClusters() {
    reopendb("reader", "reader");

    try {
      database.command("select from ouser").close();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseClass("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseCluster("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testCannotExtendClassWithNoUpdateProvileges() {
    reopendb("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");
    database.close();

    reopendb("writer", "writer");

    try {
      database.command("alter class Protected superclass OUser").close();
      Assert.fail();
    } catch (OSecurityException e) {
    } finally {
      database.close();

      reopendb("admin", "admin");
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testSuperUserCanExtendClassWithNoUpdateProvileges() {
    reopendb("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");

    try {
      database.command("alter class Protected superclass OUser").close();
    } finally {
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testGremlinExecution() throws IOException {
    if (!database.getURL().startsWith("remote:")) return;

    reopendb("admin", "admin");
    try {
      database.execute("gremlin-groovy", "g.V()").close();
    } finally {
      database.close();
    }

    reopendb("reader", "reader");
    try {
      database.execute("gremlin-groovy", "g.V()").close();
      Assert.fail("Security breach: Gremlin can be executed by reader user!");
    } catch (OSecurityException e) {
    } finally {
      database.close();
    }

    reopendb("writer", "writer");
    try {
      database.execute("gremlin-groovy", "g.V()").close();
      Assert.fail("Security breach: Gremlin can be executed by writer user!");
    } catch (OSecurityException e) {
    } finally {
      database.close();
    }
  }

  @Test
  public void testEmptyUserName() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      String userName = "";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithAllSpaces() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "  ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesOne() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = " sas";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesTwo() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "sas ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesThree() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = " sas ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSpacesInTheMiddle() {
    reopendb("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "s a s";
      security.createUser(userName, "foobar", reader);
      Assert.assertNotNull(security.getUser(userName));
      security.dropUser(userName);
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }
}
