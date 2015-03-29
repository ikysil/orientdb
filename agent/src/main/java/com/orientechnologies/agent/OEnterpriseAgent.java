/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent;

import java.util.Map;

import com.orientechnologies.agent.hook.OAuditingHook;
import com.orientechnologies.agent.http.command.OServerCommandConfiguration;
import com.orientechnologies.agent.http.command.OServerCommandGetDeployDb;
import com.orientechnologies.agent.http.command.OServerCommandGetDistributed;
import com.orientechnologies.agent.http.command.OServerCommandGetLog;
import com.orientechnologies.agent.http.command.OServerCommandGetProfiler;
import com.orientechnologies.agent.http.command.OServerCommandPostBackupDatabase;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

public class OEnterpriseAgent extends OServerPluginAbstract implements ODatabaseLifecycleListener {
  public static final String  EE                         = "ee.";
  private static final String ORIENDB_ENTERPRISE_VERSION = "2.1"; // CHECK IF THE ORIENTDB COMMUNITY EDITION STARTS WITH THIS
  private OServer             server;
  private String              license;
  private String              version;
  private boolean             enabled                    = false;

  public OEnterpriseAgent() {
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
    for (OServerParameterConfiguration p : iParams) {
      if (p.name.equals("license"))
        license = p.value;
      if (p.name.equals("version"))
        version = p.value;
    }
  }

  @Override
  public String getName() {
    return "enterprise-agent";
  }

  @Override
  public void startup() {
    if (checkLicense()) {
      enabled = true;
      installProfiler();
      installCommands();

      Thread installer = new Thread(new Runnable() {
        @Override
        public void run() {

          int retry = 0;
          while (true) {
            ODistributedServerManager manager = OServerMain.server().getDistributedManager();
            if (manager == null) {
              if (retry == 5) {
                break;
              }
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              retry++;
              continue;
            } else {
              Map<String, Object> map = manager.getConfigurationMap();
              String pwd = OServerMain.server().getConfiguration().getUser("root").password;
              try {
                String enc = OL.encrypt(pwd);
                map.put(EE + manager.getLocalNodeName(), enc);
              } catch (Exception e) {
                e.printStackTrace();
              }

              break;
            }

          }

        }
      });

      installer.setDaemon(true);
      installer.start();
      Orient.instance().addDbLifecycleListener(this);
    }
  }

  @Override
  public void shutdown() {
    if (enabled) {
      uninstallCommands();
      uninstallProfiler();
      Orient.instance().removeDbLifecycleListener(this);
    }
  }

  @Override
  public PRIORITY getPriority() {
    return null;
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    // REGISTER AUDITING
    final ODocument auditingCfg = new ODocument();
    final OAuditingHook auditing = new OAuditingHook(auditingCfg);
    iDatabase.registerHook(auditing);
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  private void installCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandGetProfiler());
    listener.registerStatelessCommand(new OServerCommandGetDistributed());
    listener.registerStatelessCommand(new OServerCommandGetLog());
    listener.registerStatelessCommand(new OServerCommandConfiguration());
    listener.registerStatelessCommand(new OServerCommandPostBackupDatabase());
    listener.registerStatelessCommand(new OServerCommandGetDeployDb());
  }

  private void uninstallCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.unregisterStatelessCommand(OServerCommandGetProfiler.class);
    listener.unregisterStatelessCommand(OServerCommandGetDistributed.class);
    listener.unregisterStatelessCommand(OServerCommandGetLog.class);
    listener.unregisterStatelessCommand(OServerCommandConfiguration.class);
    listener.unregisterStatelessCommand(OServerCommandPostBackupDatabase.class);
    listener.unregisterStatelessCommand(OServerCommandGetDeployDb.class);
  }

  private void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OEnterpriseProfiler(60, 24, currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private void uninstallProfiler() {
    final OProfilerMBean currentProfiler = Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OProfiler((OProfiler) currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private boolean checkLicense() {

    // final int dayLeft = OL.checkDate(license);
    final int dayLeft = 50;

    System.out.printf("\n\n********************************************************************");
    System.out.printf("\n*                 ORIENTDB  -  ENTERPRISE EDITION                  *");
    System.out.printf("\n*                                                                  *");
    System.out.printf("\n*            " + OConstants.COPYRIGHT + "           *");
    System.out.printf("\n********************************************************************");
    System.out.printf("\n* Version...: %-52s *", ORIENDB_ENTERPRISE_VERSION);
    // System.out.printf("\n* License...: %-52s *", license);
    // if (dayLeft < 0) {
    // System.out.printf("\n* Licence expired since: %03d days                                  *", Math.abs(dayLeft));
    // System.out.printf("\n* Enterprise features will be disabled in : %03d days               *", OL.DELAY + dayLeft);
    // System.out.printf("\n* Please contact Orient Technologies at: info@orientechonogies.com *");
    // } else {
    // System.out.printf("\n* Expires in: %03d days                                             *", dayLeft);
    // }

    System.out.printf("\n********************************************************************\n");

    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.license"), "Enterprise License",
            METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return license;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.agentVersion"), "Enterprise License",
            METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return ORIENDB_ENTERPRISE_VERSION;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.dayLeft"), "Enterprise License Day Left",
            METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return dayLeft;
              }
            });

    return true;
  }
}
