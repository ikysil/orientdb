package com.orientechnologies.security.password;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.security.BaseServerSecurityTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EnterprisePasswordTest extends BaseServerSecurityTest {

  @Ignore
  @Test
  public void passwordValidationTest() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    try {
      ODatabaseSession session = remote.open(name.getMethodName(), "admin", "admin");
      session.command("create user fake identified by fake role admin").close();
      Assert.fail("It should not set the password");
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }
  }
}
