package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Test;

public class CreateClassTest extends BaseMemoryDatabase {

  @Test
  public void testCreateClassSQL() {
    db.command("drop class V").close();
    db.command("create class V clusters 16").close();
    db.command("create class X extends V clusters 32").close();

    final OClass clazzV = db.getMetadata().getSchema().getClass("V");
    assertEquals(16, clazzV.getClusterIds().length);

    final OClass clazzX = db.getMetadata().getSchema().getClass("X");
    assertEquals(32, clazzX.getClusterIds().length);
  }

  @Test
  public void testCreateClassSQLSpecifiedClusters() {
    int s = db.addCluster("second");
    int t = db.addCluster("third");
    db.command("drop class V").close();
    db.command("create class V cluster " + s + "," + t).close();

    final OClass clazzV = db.getMetadata().getSchema().getClass("V");
    assertEquals(2, clazzV.getClusterIds().length);

    assertEquals(s, clazzV.getClusterIds()[0]);
    assertEquals(t, clazzV.getClusterIds()[1]);
  }

  @Test
  public void testCreateClassIfNotExists() {
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass("TestCreateClass");
    boolean result = schema.createClassIfNotExists("TestCreateClassNotExists");
    assertTrue(result);
    assertNotNull(schema.getClass("TestCreateClassNotExists"));

    result = schema.createClassIfNotExists("TestCreateClass");
    assertFalse(result);
  }

  @Test
  public void testCreateClassIfNotExistsSuperclass() {
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass("TestCreateClass");
    boolean result =
        schema.createClassIfNotExists("TestCreateClassNotExists", schema.getClass("V"));
    assertTrue(result);
    assertNotNull(schema.getClass("TestCreateClassNotExists"));
    assertEquals(1, schema.getClass("TestCreateClassNotExists").getSuperClasses().size());

    result = schema.createClassIfNotExists("TestCreateClass");
    assertFalse(result);
  }
}
