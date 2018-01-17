package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.backup.ListBackupCommandProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.AbstractEnterpriseServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupList;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupMode;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupModeConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class BackupCommandProcessorDistributedTest extends AbstractEnterpriseServerClusterTest {

  private final String DB_NAME     = "backupDB";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target") + File.separator + "databases" + File.separator + DB_NAME;

  //  @Before
  //  public void bootOrientDB() throws Exception {
  //    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
  //
  //    InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
  //    server = OServerMain.create(false);
  //    server.startup(stream);
  //
  //    OrientDB orientDB = server.getContext();
  //
  //    if (orientDB.exists(DB_NAME))
  //      orientDB.drop(DB_NAME);
  //
  //    if (orientDB.exists(NEW_DB_NAME))
  //      orientDB.drop(NEW_DB_NAME);
  //
  //    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);
  //
  //    server.activate();
  //
  //    server.getSystemDatabase().executeInDBScope(iArgument -> {
  //
  //      iArgument.command("delete from OBackupLog");
  //      return null;
  //    });
  //
  //    agent = server.getPluginByClass(OEnterpriseAgent.class);
  //
  //    deleteBackupConfig();
  //
  //  }
  //
  //  @After
  //  public void tearDownOrientDB() {
  //
  //    deleteBackupConfig();
  //
  //    OrientDB orientDB = server.getContext();
  //    if (orientDB.exists(DB_NAME))
  //      orientDB.drop(DB_NAME);
  //
  //    if (orientDB.exists(NEW_DB_NAME))
  //      orientDB.drop(NEW_DB_NAME);
  //
  //    if (server != null)
  //      server.shutdown();
  //
  //    Orient.instance().shutdown();
  //    Orient.instance().startup();
  //
  //    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
  //
  //  }

  private void deleteBackupConfig(OEnterpriseAgent agent) {
    ODocument configuration = agent.getBackupManager().getConfiguration();

    configuration.<List<ODocument>>field("backups").stream().map(cfg -> cfg.<String>field("uuid")).collect(Collectors.toList())
        .forEach((b) -> agent.getBackupManager().removeAndStopBackup(b));
  }

  @Test
  public void testBackupCommandProcessorEmptyBackups() throws Exception {

    execute(2, () -> {

      ServerRun serverRun = this.serverInstance.get(0);

      BackupList payload = getBackupList(serverRun.getNodeName());

      assertThat(payload.getBackups()).hasSize(0);

      return null;
    });

  }

  @Test
    public void testBackupCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun serverRun = this.serverInstance.get(0);
      ServerRun serverRun1 = this.serverInstance.get(0);

      ODocument cfg = createBackupConfig(getAgent(serverRun.getNodeName()));

      String uuid = cfg.field("uuid");

      BackupList payload = getBackupList(serverRun1.getNodeName());

      assertThat(payload.getBackups()).hasSize(1);

      BackupInfo backupInfo = payload.getBackups().get(0);

      assertThat(backupInfo.getUuid()).isEqualTo(uuid);

      assertThat(backupInfo.getDbName()).isEqualTo(DB_NAME);

      assertThat(backupInfo.getDirectory()).isEqualTo(BACKUP_PATH);

      assertThat(backupInfo.getEnabled()).isEqualTo(true);

      assertThat(backupInfo.getRetentionDays()).isEqualTo(30);

      assertThat(backupInfo.getModes()).containsKeys(BackupMode.FULL_BACKUP);
      assertThat(backupInfo.getModes()).containsKeys(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig incremental = backupInfo.getModes().get(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig full = backupInfo.getModes().get(BackupMode.FULL_BACKUP);

      assertThat(full.getWhen()).isEqualTo("0/5 * * * * ?");
      assertThat(incremental.getWhen()).isEqualTo("0/2 * * * * ?");

      // Add second config the the second node
      createBackupConfig(getAgent(serverRun1.getNodeName()));

      payload = getBackupList(serverRun.getNodeName());

      assertThat(payload.getBackups()).hasSize(2);

      return null;
    });

  }

  private ODocument createBackupConfig(OEnterpriseAgent agent) {
    ODocument modes = new ODocument();

    ODocument mode = new ODocument();
    modes.field("FULL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument incrementalMode = new ODocument();
    modes.field("INCREMENTAL_BACKUP", incrementalMode);
    incrementalMode.field("when", "0/2 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    return agent.getBackupManager().addBackup(backup);
  }

  private BackupList getBackupList(String nodeName) {

    OEnterpriseAgent agent = getAgent(nodeName);
    ListBackupCommandProcessor backupCommandProcessor = new ListBackupCommandProcessor();

    Command command = new Command();
    command.setId("test");
    command.setPayload("");
    command.setResponseChannel("channelTest");

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    Assert.assertTrue(execute.getPayload() instanceof BackupList);

    return (BackupList) execute.getPayload();
  }

  private OEnterpriseAgent getAgent(String server) {

    return this.serverInstance.stream().filter(serverRun -> serverRun.getNodeName().equals(server)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(String.format("Cannot find server with name %s", server)))
        .getServerInstance().getPluginByClass(OEnterpriseAgent.class);

  }
  //
  //  @Test
  //  public void testBackupCommandProcessor() {
  //
  //    ODocument cfg = createBackupConfig();
  //
  //    String uuid = cfg.field("uuid");
  //
  //    BackupList payload = getBackupList();
  //
  //    assertThat(payload.getBackups()).hasSize(1);
  //
  //    BackupInfo backupInfo = payload.getBackups().get(0);
  //
  //    assertThat(backupInfo.getUuid()).isEqualTo(uuid);
  //
  //    assertThat(backupInfo.getDbName()).isEqualTo(DB_NAME);
  //
  //    assertThat(backupInfo.getDirectory()).isEqualTo(BACKUP_PATH);
  //
  //    assertThat(backupInfo.getEnabled()).isEqualTo(true);
  //
  //    assertThat(backupInfo.getRetentionDays()).isEqualTo(30);
  //
  //    assertThat(backupInfo.getModes()).containsKeys(BackupMode.FULL_BACKUP);
  //    assertThat(backupInfo.getModes()).containsKeys(BackupMode.INCREMENTAL_BACKUP);
  //
  //    BackupModeConfig incremental = backupInfo.getModes().get(BackupMode.INCREMENTAL_BACKUP);
  //
  //    BackupModeConfig full = backupInfo.getModes().get(BackupMode.FULL_BACKUP);
  //
  //    assertThat(full.getWhen()).isEqualTo("0/5 * * * * ?");
  //    assertThat(incremental.getWhen()).isEqualTo("0/2 * * * * ?");
  //
  //  }
  //
  //  private ODocument createBackupConfig() {
  //    ODocument modes = new ODocument();
  //
  //    ODocument mode = new ODocument();
  //    modes.field("FULL_BACKUP", mode);
  //    mode.field("when", "0/5 * * * * ?");
  //
  //    ODocument incrementalMode = new ODocument();
  //    modes.field("INCREMENTAL_BACKUP", incrementalMode);
  //    incrementalMode.field("when", "0/2 * * * * ?");
  //
  //    ODocument backup = new ODocument();
  //    backup.field("dbName", DB_NAME);
  //    backup.field("directory", BACKUP_PATH);
  //    backup.field("modes", modes);
  //    backup.field("enabled", true);
  //    backup.field("retentionDays", 30);
  //
  //    return agent.getBackupManager().addBackup(backup);
  //  }
  //
  //  @Test
  //  public void testAddBackupCommandProcessor() {
  //
  //    BackupInfo info = new BackupInfo();
  //    info.setDbName(DB_NAME);
  //    info.setDirectory(BACKUP_PATH);
  //    info.setEnabled(true);
  //    info.setRetentionDays(30);
  //
  //    info.setModes(new HashMap<BackupMode, BackupModeConfig>() {
  //      {
  //        put(BackupMode.FULL_BACKUP, new BackupModeConfig("0/5 * * * * ?"));
  //        put(BackupMode.INCREMENTAL_BACKUP, new BackupModeConfig("0/2 * * * * ?"));
  //      }
  //    });
  //
  //    Command command = new Command();
  //    command.setId("test");
  //    command.setPayload(info);
  //    command.setResponseChannel("channelTest");
  //
  //    AddBackupCommandProcessor backupCommandProcessor = new AddBackupCommandProcessor();
  //
  //    CommandResponse execute = backupCommandProcessor.execute(command, agent);
  //
  //    Assert.assertTrue(execute.getPayload() instanceof BackupInfo);
  //
  //    BackupInfo backupInfo = (BackupInfo) execute.getPayload();
  //
  //    assertThat(backupInfo.getUuid()).isNotNull();
  //
  //    assertThat(backupInfo.getDbName()).isEqualTo(DB_NAME);
  //
  //    assertThat(backupInfo.getDirectory()).isEqualTo(BACKUP_PATH);
  //
  //    assertThat(backupInfo.getEnabled()).isEqualTo(true);
  //
  //    assertThat(backupInfo.getRetentionDays()).isEqualTo(30);
  //
  //    assertThat(backupInfo.getModes()).containsKeys(BackupMode.FULL_BACKUP);
  //    assertThat(backupInfo.getModes()).containsKeys(BackupMode.INCREMENTAL_BACKUP);
  //
  //    BackupModeConfig incremental = backupInfo.getModes().get(BackupMode.INCREMENTAL_BACKUP);
  //
  //    BackupModeConfig full = backupInfo.getModes().get(BackupMode.FULL_BACKUP);
  //
  //    assertThat(full.getWhen()).isEqualTo("0/5 * * * * ?");
  //    assertThat(incremental.getWhen()).isEqualTo("0/2 * * * * ?");
  //
  //  }
  //
  //  @Test
  //  public void testRemoveBackupCommandProcessor() {
  //
  //    ODocument cfg = createBackupConfig();
  //
  //    BackupList backupList = getBackupList();
  //
  //    assertThat(backupList.getBackups()).hasSize(1);
  //
  //    String uuid = cfg.field("uuid");
  //
  //    Command command = new Command();
  //    command.setId("test");
  //    command.setPayload(new BackupLogRequest(uuid));
  //    command.setResponseChannel("channelTest");
  //
  //    RemoveBackupCommandProcessor backupCommandProcessor = new RemoveBackupCommandProcessor();
  //
  //    CommandResponse execute = backupCommandProcessor.execute(command, agent);
  //
  //    backupList = getBackupList();
  //
  //    assertThat(backupList.getBackups()).hasSize(0);
  //
  //  }
  //
  //  @Test
  //  public void testListLogsCommandProcessor() throws InterruptedException {
  //
  //    ODocument cfg = createBackupConfig();
  //
  //    BackupList backupList = getBackupList();
  //
  //    assertThat(backupList.getBackups()).hasSize(1);
  //
  //    String uuid = cfg.field("uuid");
  //
  //    final OBackupTask task = agent.getBackupManager().getTask(uuid);
  //
  //    final CountDownLatch latch = new CountDownLatch(3);
  //    task.registerListener((cfg1, log) -> {
  //      latch.countDown();
  //      return latch.getCount() > 0;
  //
  //    });
  //    latch.await();
  //
  //    BackupLogsList backupLogsList = getBackupLogList(uuid);
  //
  //    assertThat(backupLogsList.getLogs()).hasSize(4);
  //
  //  }
  //
  //  private BackupLogsList getBackupLogList(String uuid) {
  //    Command command = new Command();
  //    command.setId("test");
  //    command.setPayload(new BackupLogRequest(uuid, null, null, new HashMap<>()));
  //    command.setResponseChannel("channelTest");
  //
  //    ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();
  //
  //    CommandResponse execute = backupCommandProcessor.execute(command, agent);
  //
  //    return (BackupLogsList) execute.getPayload();
  //  }
  //
  //  @Test
  //  public void testListLogsWithUnitIdCommandProcessor() throws InterruptedException {
  //
  //    ODocument cfg = createBackupConfig();
  //
  //    BackupList backupList = getBackupList();
  //
  //    assertThat(backupList.getBackups()).hasSize(1);
  //
  //    String uuid = cfg.field("uuid");
  //
  //    final OBackupTask task = agent.getBackupManager().getTask(uuid);
  //
  //    AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
  //    final CountDownLatch latch = new CountDownLatch(3);
  //    task.registerListener((cfg1, log) -> {
  //      latch.countDown();
  //      lastLog.set(log);
  //      return latch.getCount() > 0;
  //
  //    });
  //    latch.await();
  //
  //    Command command = new Command();
  //    command.setId("test");
  //    command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), null, null, new HashMap<String, String>() {{
  //      put("op", "BACKUP_FINISHED");
  //    }}));
  //    command.setResponseChannel("channelTest");
  //
  //    ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();
  //
  //    CommandResponse execute = backupCommandProcessor.execute(command, agent);
  //
  //    BackupLogsList backupLogsList = (BackupLogsList) execute.getPayload();
  //
  //    assertThat(backupLogsList.getLogs()).hasSize(1);
  //
  //  }
  //
  //  @Test
  //  public void testRemoveBackupLogsCommandProcessor() throws InterruptedException {
  //
  //    ODocument cfg = createBackupConfig();
  //
  //    BackupList backupList = getBackupList();
  //
  //    assertThat(backupList.getBackups()).hasSize(1);
  //
  //    String uuid = cfg.field("uuid");
  //
  //    final OBackupTask task = agent.getBackupManager().getTask(uuid);
  //
  //    AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
  //    final CountDownLatch latch = new CountDownLatch(3);
  //    task.registerListener((cfg1, log) -> {
  //      latch.countDown();
  //
  //      if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
  //        lastLog.set(log);
  //      }
  //      return latch.getCount() > 0;
  //
  //    });
  //    latch.await();
  //
  //    Command command = new Command();
  //    command.setId("test");
  //    command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), lastLog.get().getTxId()));
  //    command.setResponseChannel("channelTest");
  //
  //    RemoveBackupCommandProcessor remove = new RemoveBackupCommandProcessor();
  //
  //    CommandResponse execute = remove.execute(command, agent);
  //
  //    String result = (String) execute.getPayload();
  //
  //    BackupLogsList backupLogsList = getBackupLogList(uuid);
  //
  //    assertThat(backupLogsList.getLogs()).hasSize(0);
  //
  //  }

  @Override
  protected String getDatabaseName() {
    return DB_NAME;
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "orientdb-distributed-server-config-" + server.getServerId() + ".xml";
  }

}
