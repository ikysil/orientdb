/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Collection;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

public class OLuceneOverlapOperator extends OLuceneSpatialOperator {

  public OLuceneOverlapOperator() {
    super("&&", 5, false);
  }

  @Override
  public boolean evaluate(Object iLeft, Object iRight, OCommandContext ctx) {
    Shape shape = factory.fromObject(iLeft);

    // TODO { 'shape' : { 'type' : 'LineString' , 'coordinates' : [[1,2],[4,6]]} }
    // TODO is not translated in map but in array[ { 'type' : 'LineString' , 'coordinates' :
    // [[1,2],[4,6]]} ]
    Object filter;
    if (iRight instanceof Collection) {
      filter = ((Collection) iRight).iterator().next();
    } else {
      filter = iRight;
    }
    Shape shape1 = factory.fromObject(filter);

    return SpatialOperation.BBoxIntersects.evaluate(shape, shape1.getBoundingBox());
  }
}
