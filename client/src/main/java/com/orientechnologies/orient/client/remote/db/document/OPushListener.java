package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OPushListener implements OMetadataPushListener {

  private OSharedContextRemote ctx;

  public OPushListener(OSharedContextRemote ctx) {
    this.ctx = ctx;
  }

  @Override
  public void updateFunction() {
    ctx.getFunctionLibrary().update();
  }

  @Override
  public void updateIndexManager(ODocument indexes) {
    ((OIndexManagerRemote) ctx.getIndexManager()).update(indexes);
  }

  @Override
  public void updateSequences() {
    ctx.getSequenceLibrary().update();
  }

  @Override
  public void updateSchema(ODocument schema) {
    ctx.getSchema().update(schema);
  }
}
