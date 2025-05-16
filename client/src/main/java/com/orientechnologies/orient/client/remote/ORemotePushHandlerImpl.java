package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.db.document.OMetadataPushListener;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import java.util.Map;

public class ORemotePushHandlerImpl implements ORemotePushHandler {

  private ORemoteClient client;
  private OMetadataPushListener metadataHandler;

  public ORemotePushHandlerImpl(ORemoteClient client, OMetadataPushListener handler) {
    this.client = client;
    this.metadataHandler = handler;
  }

  @Override
  public OChannelBinary getNetwork(String host) {
    return client.getNetwork(host);
  }

  @Override
  public OBinaryPushRequest createPush(byte push) {
    return client.createPush(push);
  }

  @Override
  public void executeLiveQueryPush(OLiveQueryPushRequest pushRequest) {
    Map<Integer, OLiveQueryClientListener> listeners = client.getLiveQueryListener();
    OLiveQueryClientListener listener = listeners.get(pushRequest.getMonitorId());
    if (listener.onEvent(pushRequest)) {
      listeners.remove(pushRequest.getMonitorId());
    }
  }

  @Override
  public void onPushReconnect(String host) {
    client.onPushReconnect(host);
  }

  @Override
  public void onPushDisconnect(OChannelBinary network, Exception e) {
    client.onPushDisconnect(network, e);
  }

  @Override
  public void returnSocket(OChannelBinary network) {
    client.returnSocket(network);
  }

  public OBinaryPushResponse executeUpdateDistributedConfig(
      OPushDistributedConfigurationRequest request) {
    client
        .getRemoteURLs()
        .updateDistributedNodes(
            request.getHosts(), client.getConfiguration().getContextConfiguration());
    return null;
  }

  public OBinaryPushResponse executeUpdateFunction(OPushFunctionsRequest request) {
    metadataHandler.updateFunction();
    return null;
  }

  public OBinaryPushResponse executeUpdateSequences(OPushSequencesRequest request) {
    metadataHandler.updateSequences();
    return null;
  }

  public OBinaryPushResponse executeUpdateStorageConfig(OPushStorageConfigurationRequest payload) {
    client.updateStorageConfiguration(payload.getPayload());
    return null;
  }

  public OBinaryPushResponse executeUpdateSchema(OPushSchemaRequest request) {
    metadataHandler.updateSchema(request.getSchema());
    return null;
  }

  public OBinaryPushResponse executeUpdateIndexManager(OPushIndexManagerRequest request) {
    metadataHandler.updateIndexManager(request.getIndexManager());
    return null;
  }
}
