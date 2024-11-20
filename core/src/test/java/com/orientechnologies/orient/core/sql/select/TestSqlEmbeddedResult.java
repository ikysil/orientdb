package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlEmbeddedResult extends BaseMemoryDatabase {

  @Test
  public void testEmbeddedRusultTypeNotLink() {
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument();
    doc1.field("format", 1);
    Set<ODocument> docs = new HashSet<ODocument>();
    docs.add(doc1);
    doc.field("rel", docs);
    // doc
    db.save(doc);

    List<OResult> res =
        db
            .query(
                "select $Pics[0] as el FROM Test LET $Pics = (select expand( rel.include('format'))"
                    + " from $current)")
            .stream()
            .toList();
    Assert.assertEquals(res.size(), 1);
    OResult ele = res.get(0);
    Assert.assertNotNull(ele.getProperty("el"));
    Assert.assertTrue(ele.getProperty("el") instanceof OResult);
    Assert.assertTrue(((OResult) ele.getProperty("el")).getIdentity().isEmpty());

    res =
        db
            .query(
                "select $Pics as el FROM Test LET $Pics = (select expand( rel.include('format'))"
                    + " from $current)")
            .stream()
            .toList();

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertNotNull(ele.getProperty("el"));
    Assert.assertTrue(ele.getProperty("el") instanceof List<?>);
    Assert.assertTrue(((List) ele.getProperty("el")).get(0) instanceof OResult);
    Assert.assertTrue(((List<OResult>) ele.getProperty("el")).get(0).getIdentity().isEmpty());
  }
}
