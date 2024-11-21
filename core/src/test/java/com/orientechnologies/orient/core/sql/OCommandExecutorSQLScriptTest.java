package com.orientechnologies.orient.core.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OCommandExecutorSQLScriptTest extends BaseMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();

    db.command("CREATE class foo").close();

    db.command("insert into foo (name, bar) values ('a', 1)").close();
    db.command("insert into foo (name, bar) values ('b', 2)").close();
    db.command("insert into foo (name, bar) values ('c', 3)").close();

    db.activateOnCurrentThread();
  }

  @Test
  public void testQuery() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("begin;");
    script.append("let $a = select from foo;");
    script.append("commit;");
    script.append("return $a;");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.size(), 3);
  }

  @Test
  public void testTx() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("begin isolation REPEATABLE_READ;");
    script.append("let $a = insert into V set test = 'sql script test';");
    script.append("commit retry 10;");
    script.append("return $a;");
    OResultSet qResult = db.execute("sql", script.toString());

    Assert.assertNotNull(qResult.hashCode());
    qResult.close();
  }

  @Test
  public void testReturnExpanded() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("let $a = insert into V set test = 'sql script test';");
    script.append("return $a.toJSON() ");

    List<OResult> qResultSet = db.execute("sql", script.toString()).stream().toList();
    String qResult = qResultSet.get(0).getProperty("value");
    Assert.assertNotNull(qResult);

    new ODocument().fromJSON(qResult.substring(1, qResult.length() - 1));

    script = new StringBuilder();
    script.append("let $a = select from V limit 2;");
    script.append("return $a.toJSON() ;");
    List<OResult> resultSet = db.execute("sql", script.toString()).stream().toList();

    String result = resultSet.get(0).getProperty("value");
    Assert.assertNotNull(result);
    result = result.trim();
    Assert.assertTrue(result.startsWith("["));
    Assert.assertTrue(result.endsWith("]"));
    new ODocument().fromJSON(result.substring(1, result.length() - 1));
  }

  @Test
  public void testSleep() throws Exception {
    long begin = System.currentTimeMillis();

    StringBuilder script = new StringBuilder();
    script.append("sleep 500");
    db.execute("sql", script.toString()).close();

    Assert.assertTrue(System.currentTimeMillis() - begin >= 500);
  }

  @Test
  public void testConsoleLog() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'log';");
    script.append("console.log 'This is a test of log for ${a}'");
    db.execute("sql", script.toString()).close();
  }

  @Test
  public void testConsoleOutput() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'output';");
    script.append("console.output 'This is a test of log for ${a}'");
    db.execute("sql", script.toString()).close();
  }

  @Test
  public void testConsoleError() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'error';");
    script.append("console.error 'This is a test of log for ${a}'");
    db.execute("sql", script.toString()).close();
  }

  @Test
  public void testReturnObject() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("return [{ a: 'b' }];");
    OResultSet result = db.execute("sql", script.toString());

    OResult res = result.next();
    Assert.assertTrue(res.getProperty("value") instanceof List);
    Assert.assertTrue(((List) res.getProperty("value")).get(0) instanceof Map);
  }

  @Test
  public void testIncrementAndLet() throws Exception {

    StringBuilder script = new StringBuilder();
    script.append("CREATE CLASS TestCounter;");
    script.append("INSERT INTO TestCounter set weight = 3;\n");
    script.append("LET counter = SELECT count(*) FROM TestCounter;\n");
    script.append("UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n");
    List<ODocument> qResult = db.command(new OCommandScript("sql", script.toString())).execute();

    assertThat(qResult.get(0).<Long>field("weight")).isEqualTo(4L);
  }

  @Test
  @Ignore
  public void testIncrementAndLetNewApi() throws Exception {

    StringBuilder script = new StringBuilder();
    script.append("CREATE CLASS TestCounter;\n");
    script.append("INSERT INTO TestCounter set weight = 3;\n");
    script.append("LET counter = SELECT count(*) FROM TestCounter;\n");
    script.append("UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n");
    OResultSet qResult = db.execute("sql", script.toString());

    assertThat(qResult.next().getElement().get().<Long>getProperty("weight")).isEqualTo(4L);
  }

  @Test
  public void testIf1() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;");
    script.append("if($a[0].one = 1){");
    script.append(" return 'OK' ;");
    script.append("}");
    script.append("return 'FAIL' ;");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testIf2() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;");
    script.append("if    ($a[0].one = 1)   { ");
    script.append(" return 'OK' ;");
    script.append("     }      ");
    script.append("return 'FAIL';");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testIf3() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("let $a = select 1 as one; if($a[0].one = 1){return 'OK';}return 'FAIL';");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testNestedIf2() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;");
    script.append("if($a[0].one = 1){");
    script.append("    if($a[0].one = 'zz'){");
    script.append("      return 'FAIL';");
    script.append("    }");
    script.append("  return 'OK';");
    script.append("}\n");
    script.append("return 'FAIL';");

    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testNestedIf3() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one ;\n");
    script.append("if($a[0].one = 'zz'){\n");
    script.append("    if($a[0].one = 1){\n");
    script.append("      return 'FAIL' ;\n");
    script.append("    }\n");
    script.append("  return 'FAIL' ; \n");
    script.append("}\n");
    script.append("return 'OK' ; \n");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testIfRealQuery() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select from foo;\n");
    script.append("if($a is not null and $a.size() = 3){\n");
    script.append("  return $a ;\n");
    script.append("}\n");
    script.append("return 'FAIL';\n");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult.size(), 3);
  }

  @Test
  public void testIfMultipleStatements() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if($a[0].one = 1){\n");
    script.append("  let $b = select 'OK' as ok;\n");
    script.append("  return $b[0].ok; \n");
    script.append("}\n");
    script.append("return 'FAIL';");
    List<OResult> qResult = db.execute("sql", script.toString()).stream().toList();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult.get(0).getProperty("value"), "OK");
  }

  @Test
  public void testScriptSubContext() throws Exception {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select from foo limit 1;");
    script.append("select from (traverse doesnotexist from $a)\n");
    OResultSet qResult = db.execute("sql", script.toString());

    Assert.assertTrue(qResult.hasNext());
    qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testSemicolonInString() throws Exception {
    // issue https://github.com/orientechnologies/orientjs/issues/133
    // testing parsing problem
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 'foo ; bar' as one;");
    script.append("let $b = select 'foo \\\'; bar' as one;");

    script.append("let $a = select \"foo ; bar\" as one;");
    script.append("let $b = select \"foo \\\"; bar\" as one;");
    db.execute("sql", script.toString()).close();
  }

  @Test
  public void testQuotedRegex() {
    // issue #4996 (simplified)
    db.command("CREATE CLASS QuotedRegex2").close();
    String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

    db.execute("sql", batch.toString()).close();

    OResultSet result = db.query("SELECT FROM QuotedRegex2");
    OResult doc = result.next();
    Assert.assertEquals(doc.getProperty("regexp"), "'';");
  }

  @Test
  public void testParameters1() {
    String className = "testParameters1";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = :name;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = :_name2;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    OResultSet rs = db.execute("sql", script, map);
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }

  @Test
  public void testPositionalParameters() {
    String className = "testPositionalParameters";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    OResultSet rs = db.execute("sql", script, "bozo", "bozi");
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }
}
