/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;
import org.pentaho.metastore.persist.MetaStoreFactory;

import javax.cache.Cache;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.pentaho.metastore.util.PentahoDefaults.NAMESPACE;

public class DataServiceMetaStoreUtil {
  private final List<PushDownFactory> pushDownFactories;
  private Cache<String, DataServiceMeta> cache;

  public DataServiceMetaStoreUtil( List<PushDownFactory> pushDownFactories,
                                   Cache<String, DataServiceMeta> cache ) {
    this.pushDownFactories = pushDownFactories;
    this.cache = cache;
  }

  public DataServiceMeta getDataService( String serviceName, Repository repository, IMetaStore metaStore )
    throws MetaStoreException, KettleException {
    DataServiceMeta dataService = cache.get( serviceName );
    if ( dataService != null ) {
      return dataService;
    }
    ServiceTrans transReference = getServiceTransFactory( metaStore ).loadElement( serviceName );
    if ( transReference == null ) {
      throw new MetaStoreException( MessageFormat.format( "Data Service {0} not found", serviceName ) );
    }
    TransMeta serviceTrans = transReference.getReference().load( repository );
    return getDataService( serviceName, serviceTrans );
  }

  public DataServiceMeta getDataService( String serviceName, TransMeta serviceTrans ) throws MetaStoreException {
    DataServiceMeta dataService = cache.get( serviceName );
    if ( dataService != null && dataService.getServiceTrans() == serviceTrans ) {
      return dataService;
    }
    DataServiceMeta dataServiceMeta = getDataServiceFactory( serviceTrans ).loadElement( serviceName );
    if ( dataServiceMeta == null ) {
      throw new MetaStoreException( MessageFormat.format( "Data Service {0} not found in transformation {1}",
        serviceName, serviceTrans.getName() ) );
    }
    dataServiceMeta.setServiceTrans( serviceTrans );
    addToCache( dataServiceMeta );
    return dataServiceMeta;
  }

  public List<DataServiceMeta> getDataServices( Repository repository, IMetaStore metaStore )
    throws MetaStoreException {
    Multimap<ServiceTrans.Reference, ServiceTrans> transMap = getServiceTransMap( metaStore );

    List<DataServiceMeta> dataServices = Lists.newArrayListWithExpectedSize( transMap.size() );
    for ( ServiceTrans.Reference transReference : transMap.keySet() ) {
      try {
        TransMeta transMeta = transReference.load( repository );
        dataServices.addAll( getDataServices( transMeta ) );
      } catch ( KettleException ignored ) {
        // Ignore transformations we cannot access
      }
    }

    return dataServices;
  }

  public List<DataServiceMeta> getDataServices( TransMeta transMeta ) throws MetaStoreException {
    List<DataServiceMeta> dataServices = getDataServiceFactory( transMeta ).getElements();
    for ( DataServiceMeta dataService : dataServices ) {
      dataService.setServiceTrans( transMeta );
      addToCache( dataService );
    }
    return dataServices;
  }

  private Multimap<ServiceTrans.Reference, ServiceTrans> getServiceTransMap( IMetaStore metaStore )
    throws MetaStoreException {
    return Multimaps.index( getServiceTransFactory( metaStore ).getElements(),
      new Function<ServiceTrans, ServiceTrans.Reference>() {
        @Override public ServiceTrans.Reference apply( ServiceTrans serviceTrans ) {
          return serviceTrans.getReference();
        }
      } );
  }

  public List<String> getDataServiceNames( IMetaStore metaStore )
    throws MetaStoreException {
    return getServiceTransFactory( metaStore ).getElementNames();
  }

  public DataServiceMeta getDataServiceByStepName( TransMeta transMeta, String stepName ) throws MetaStoreException {
    String cacheKey = DataServiceMeta.createCacheKey( transMeta, stepName );
    DataServiceMeta cachedDataService = cache.get( cacheKey );
    if ( cachedDataService != null ) {
      // Check if Data Service is still valid
      if ( cachedDataService.getServiceTrans() == transMeta && cachedDataService.getStepname().equals( stepName ) ) {
        return cachedDataService;
      } else {
        cache.remove( cacheKey, cachedDataService );
      }
    }
    // Look up from embedded metastore
    for ( DataServiceMeta dataServiceMeta : getDataServices( transMeta ) ) {
      if ( dataServiceMeta.getStepname().equalsIgnoreCase( stepName ) ) {
        return dataServiceMeta;
      }
    }
    return null;
  }

  public void save( IMetaStore metaStore, DataServiceMeta dataService )
    throws MetaStoreException {

    if ( dataService != null && dataService.isDefined() ) {
      TransMeta transMeta = checkNotNull( dataService.getServiceTrans(), "Service trans not defined for data service" );
      // Initialize optimizations
      for ( PushDownOptimizationMeta optMeta : dataService.getPushDownOptimizationMeta() ) {
        optMeta.getType().init( transMeta, dataService, optMeta );
      }

      LogChannel.GENERAL.logBasic( "Saving data service in meta store '" + transMeta.getMetaStore() + "'" );

      // Save to embedded MetaStore
      getDataServiceFactory( transMeta ).saveElement( dataService );

      // Leave trace of this transformation...
      //
      getServiceTransFactory( metaStore ).saveElement( ServiceTrans.create( dataService.getName(), transMeta ) );

      addToCache( dataService );
    }
  }

  public void removeDataService( TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService )
    throws MetaStoreException {
    getDataServiceFactory( transMeta ).deleteElement( dataService.getName() );
    getServiceTransFactory( metaStore ).deleteElement( dataService.getName() );
    cache.removeAll( dataService.createCacheKeys() );
  }

  private MetaStoreFactory<ServiceTrans> getServiceTransFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<ServiceTrans>( ServiceTrans.class, metaStore, NAMESPACE );
  }

  private MetaStoreFactory<DataServiceMeta> getDataServiceFactory( TransMeta transMeta ) {
    MetaStoreFactory<DataServiceMeta> dataServiceMetaFactory = new MetaStoreFactory<DataServiceMeta>(
      DataServiceMeta.class, transMeta.getEmbeddedMetaStore(), NAMESPACE );
    dataServiceMetaFactory.setObjectFactory( new DataServiceMetaObjectFactory() );
    return dataServiceMetaFactory;
  }

  private void addToCache( DataServiceMeta dataServiceMeta ) {
    cache.putAll( Maps.asMap( dataServiceMeta.createCacheKeys(), Functions.constant( dataServiceMeta ) ) );
  }

  private class DataServiceMetaObjectFactory implements IMetaStoreObjectFactory {
    @Override public Object instantiateClass( final String className, Map<String, String> context ) throws
      MetaStoreException {
      for ( PushDownFactory factory : pushDownFactories ) {
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
}
