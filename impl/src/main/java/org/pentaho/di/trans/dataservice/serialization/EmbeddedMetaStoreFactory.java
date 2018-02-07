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

import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import javax.cache.Cache;
import java.util.List;

/**
 * A MetaStoreFactory which uses the TransMeta.getEmbeddedMetaStore.
 * Handles the association of the deserialized DataServiceMeta with
 * the TransMeta it came from.
 * Optionally allows caching of DataService items if a Cache is passed
 * in the constructor.
 */
public class EmbeddedMetaStoreFactory extends MetaStoreFactory<DataServiceMeta> {

  private final TransMeta transMeta;
  private final Cache<Integer, String> stepCache;

  public EmbeddedMetaStoreFactory( TransMeta transMeta, List<PushDownFactory> factories, Cache<Integer, String> stepCache ) {
    super( DataServiceMeta.class, transMeta.getEmbeddedMetaStore(), PentahoDefaults.NAMESPACE );
    this.transMeta = transMeta;
    this.stepCache = stepCache;
    setObjectFactory( new DataServiceMetaObjectFactory( factories ) );
  }

  @Override public DataServiceMeta loadElement( String name ) throws MetaStoreException {
    DataServiceMeta dataServiceMeta = super.loadElement( name );
    if ( dataServiceMeta != null ) {
      dataServiceMeta.setServiceTrans( transMeta );
      saveToCache( dataServiceMeta );
    }
    return dataServiceMeta;
  }

  @Override public List<DataServiceMeta> getElements() throws MetaStoreException {
    List<DataServiceMeta> elements = super.getElements();
    for ( DataServiceMeta dataServiceMeta : elements ) {
      dataServiceMeta.setServiceTrans( transMeta );
      saveToCache( dataServiceMeta );
    }
    return elements;
  }

  @Override public void saveElement( DataServiceMeta dataServiceMeta ) throws MetaStoreException {
    super.saveElement( dataServiceMeta );
    saveToCache( dataServiceMeta );
  }

  private void saveToCache( DataServiceMeta meta ) {
    if ( stepCache != null ) {
      stepCache.putAll( DataServiceMetaStoreUtil.createCacheEntries( meta ) );
    }
  }
}
