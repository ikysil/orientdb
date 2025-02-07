package com.orientechnologies.agent.services.security;

import com.orientechnologies.agent.http.command.OServerCommandAuditing;
import com.orientechnologies.agent.http.command.OServerCommandGetSecurityConfig;
import com.orientechnologies.agent.http.command.OServerCommandPostSecurityReload;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.security.OSecuritySystem;

/** Created by Enrico Risa on 18/09/2018. */
public class OSecurityService implements OEnterpriseService {
  private static final OLogger logger = OLogManager.instance().logger(OSecurityService.class);

  private OEnterpriseServer server;

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
  }

  @Override
  public void start() {
    registerSecurityComponents();
    server.registerStatelessCommand(new OServerCommandAuditing(server));
    server.registerStatelessCommand(new OServerCommandGetSecurityConfig(server.getSecurity()));
    server.registerStatelessCommand(new OServerCommandPostSecurityReload(server.getSecurity()));
  }

  @Override
  public void stop() {
    unregisterSecurityComponents();
    server.unregisterStatelessCommand(OServerCommandAuditing.class);
    server.unregisterStatelessCommand(OServerCommandGetSecurityConfig.class);
    server.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
  }

  private void registerSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server
            .getSecurity()
            .registerSecurityClass(
                com.orientechnologies.agent.security.authenticator.OSecuritySymmetricKeyAuth.class);
        server
            .getSecurity()
            .registerSecurityClass(
                com.orientechnologies.agent.security.authenticator.OSystemSymmetricKeyAuth.class);

        OSecuritySystem security = server.getSecurity();

        // Disabled for now
        //        if (security instanceof ODefaultServerSecurity) {
        //          ((ODefaultServerSecurity) security).replacePasswordValidator(new
        // EnterprisePasswordValidator());
        //        }

      }
    } catch (Throwable th) {
      logger.error("registerSecurityComponents()", th);
    }
  }

  private void unregisterSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server
            .getSecurity()
            .unregisterSecurityClass(
                com.orientechnologies.agent.security.authenticator.OSecuritySymmetricKeyAuth.class);
        server
            .getSecurity()
            .unregisterSecurityClass(
                com.orientechnologies.agent.security.authenticator.OSystemSymmetricKeyAuth.class);
      }
    } catch (Throwable th) {
      logger.error("unregisterSecurityComponents()", th);
    }
  }
}
