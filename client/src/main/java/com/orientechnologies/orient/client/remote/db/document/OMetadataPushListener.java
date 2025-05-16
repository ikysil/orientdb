package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OMetadataPushListener {
  void updateSchema(ODocument schema);

  void updateIndexManager(ODocument indexes);

  void updateFunction();

  void updateSequences();
}
