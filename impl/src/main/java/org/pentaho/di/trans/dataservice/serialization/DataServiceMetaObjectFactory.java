/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Throwables;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used by the EmbeddedMetaStoreFactory to deserialize PushDownFactories.
 */
public class DataServiceMetaObjectFactory implements IMetaStoreObjectFactory {

  private final List<PushDownFactory> factories;

  public DataServiceMetaObjectFactory( List<PushDownFactory> factories ) {
    this.factories = factories;
  }

  @Override public Object instantiateClass( final String className, Map<String, String> objectContext ) throws
    MetaStoreException {
    for ( PushDownFactory factory : factories ) {
      if ( factory.getType().getName().equals( className ) ) {
        return factory.createPushDown();
      }
    }
    try {
      return Class.forName( className ).newInstance();
    } catch ( Throwable t ) {
      Throwables.propagateIfPossible( t, MetaStoreException.class );
      throw new MetaStoreException( t );
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    return Collections.emptyMap();
  }

}
