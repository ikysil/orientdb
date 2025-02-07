/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.listener;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashMap;
import java.util.Map;

public class OETLScriptImporterListener implements OETLImporterListener {
  private final Map<String, String> events;

  public OETLScriptImporterListener() {
    events = new HashMap<String, String>();
  }

  public OETLScriptImporterListener(final Map<String, String> iEvents) {
    events = iEvents;
  }

  @Override
  public void onBeforeFile(final ODatabaseDocument db, final OCommandContext iContext) {
    executeEvent(db, "onBeforeFile", iContext);
  }

  @Override
  public void onAfterFile(final ODatabaseDocument db, final OCommandContext iContext) {
    executeEvent(db, "onAfterFile", iContext);
  }

  @Override
  public boolean onBeforeLine(final ODatabaseDocument db, final OCommandContext iContext) {
    final Object ret = executeEvent(db, "onBeforeLine", iContext);
    if (ret != null && ret instanceof Boolean) return (Boolean) ret;
    return true;
  }

  @Override
  public void onAfterLine(final ODatabaseDocument db, final OCommandContext iContext) {
    executeEvent(db, "onAfterLine", iContext);
  }

  @Override
  public void onDump(final ODatabaseDocument db, final OCommandContext iContext) {
    executeEvent(db, "onDump", iContext);
  }

  @Override
  public void onJoinNotFound(
      ODatabaseDocument db, OCommandContext iContext, OIndex iIndex, Object iKey) {
    executeEvent(db, "onJoinNotFound", iContext);
  }

  @Override
  public void validate(ODatabaseDocument db, OCommandContext iContext, ODocument iRecord) {}

  private Object executeEvent(
      final ODatabaseDocument db, final String iEventName, final OCommandContext iContext) {
    if (events == null) return null;

    final String code = events.get(iEventName);

    if (code != null) {
      final Map<String, Object> pars = new HashMap<String, Object>();
      pars.put("task", iContext);
      pars.put("importer", this);

      return db.execute("javascript", code, pars);
    }
    return null;
  }
}
