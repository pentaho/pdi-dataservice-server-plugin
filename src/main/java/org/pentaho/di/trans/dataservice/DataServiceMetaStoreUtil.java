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

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.pentaho.di.trans.dataservice.cache.DataServiceMetaCache;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.List;
import java.util.Map;

import static org.pentaho.metastore.util.PentahoDefaults.*;

public class DataServiceMetaStoreUtil {

  private final List<PushDownFactory> pushDownFactories;
  private DataServiceMetaCache dataServiceMetaCache;

  public DataServiceMetaStoreUtil( List<PushDownFactory> pushDownFactories,
                                   DataServiceMetaCache dataServiceMetaCache ) {
    this.pushDownFactories = pushDownFactories;
    this.dataServiceMetaCache = dataServiceMetaCache;
  }

  public DataServiceMeta fromTransMeta( TransMeta transMeta, IMetaStore metaStore, String stepName )
    throws MetaStoreException {

    DataServiceMeta dataService = dataServiceMetaCache.get( transMeta, stepName );
    if ( dataService != null ) {
      return dataService.getName() != null ? dataService : null;
    }

    List<DataServiceMeta> dataServices = getMetaStoreFactory( metaStore ).getElements();
    Repository repository = transMeta.getRepository();
    for ( DataServiceMeta dataServiceMeta : dataServices ) {
      if ( !dataServiceMeta.getStepname().equals( stepName ) ) {
        continue;
      }

      if ( repository != null ) {
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          ObjectId objectId = transMeta.getObjectId();
          if ( objectId != null && objectId.getId().equals( dataServiceMeta.getTransObjectId() ) ) {
            dataService = dataServiceMeta;
          }
        }
        if ( dataService == null ) {
          String repositoryPath =
            transMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta
              .getName();
          if ( repositoryPath.equals( dataServiceMeta.getTransRepositoryPath() ) ) {
            dataService = dataServiceMeta;
          }
        }
      } else {
        String filename = transMeta.getFilename();
        if ( filename != null && filename.equals( dataServiceMeta.getTransFilename() ) ) {
          dataService = dataServiceMeta;
        }
      }
    }

    dataServiceMetaCache.put( transMeta, stepName, dataService );

    return dataService;
  }

  public void toTransMeta( TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService )
    throws MetaStoreException {

    if ( dataService != null && dataService.isDefined() ) {
      // Initialize optimizations
      for ( PushDownOptimizationMeta optMeta : dataService.getPushDownOptimizationMeta() ) {
        optMeta.getType().init( transMeta, dataService, optMeta );
      }

      LogChannel.GENERAL.logBasic( "Saving data service in meta store '" + transMeta.getMetaStore() + "'" );

      // Leave trace of this transformation...
      //
      Repository repository = transMeta.getRepository();
      if ( transMeta.getRepository() != null ) {
        dataService.setTransRepositoryPath( transMeta.getPathAndName() );
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          ObjectId oid = transMeta.getObjectId();
          dataService.setTransObjectId( oid == null ? null : oid.getId() );
        } else {
          dataService
            .setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName() );
        }
      } else {
        dataService.setTransFilename( transMeta.getFilename() );
      }
      getMetaStoreFactory( metaStore ).saveElement( dataService );
      dataServiceMetaCache.put( transMeta, dataService.getStepname(), dataService );
    }
  }

  public DataServiceMeta findByName( IMetaStore metaStore, String name ) throws MetaStoreException {
    return getMetaStoreFactory( metaStore ).loadElement( name );
  }

  public void removeDataService( TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService )
    throws MetaStoreException {
    getMetaStoreFactory( metaStore ).deleteElement( dataService.getName() );
    dataServiceMetaCache.put( transMeta, dataService.getStepname(), null );
  }

  /**
   * This method creates a new Element Type in the meta store corresponding encapsulating the Kettle Data Service meta
   * data.
   *
   * @param metaStore The store to create the element type in
   * @return The existing or new element type
   * @throws MetaStoreException
   * @throws org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException
   */
  @Deprecated
  // Should use metastoreFactory instead
  public static IMetaStoreElementType createDataServiceElementTypeIfNeeded( IMetaStore metaStore )
    throws MetaStoreException {

    if ( !metaStore.namespaceExists( NAMESPACE ) ) {
      metaStore.createNamespace( NAMESPACE );
    }

    IMetaStoreElementType elementType =
      metaStore.getElementTypeByName( NAMESPACE, KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME );
    if ( elementType == null ) {
      try {
        elementType = metaStore.newElementType( NAMESPACE );
        elementType.setName( KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME );
        elementType.setDescription( KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION );
        metaStore.createElementType( NAMESPACE, elementType );
      } catch ( MetaStoreException e ) {
        throw new MetaStoreException( "Unable to create new data service element type in the metastore", e );
      }
    }
    return elementType;
  }

  public MetaStoreFactory<DataServiceMeta> getMetaStoreFactory( IMetaStore metaStore ) {
    MetaStoreFactory<DataServiceMeta> dataServiceMetaFactory = new MetaStoreFactory<DataServiceMeta>(
      DataServiceMeta.class, metaStore, NAMESPACE );
    dataServiceMetaFactory.setObjectFactory( objectFactory );
    return dataServiceMetaFactory;
  }

  public List<PushDownFactory> getPushDownFactories() {
    return pushDownFactories;
  }

  private final IMetaStoreObjectFactory objectFactory = new IMetaStoreObjectFactory() {
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
      return Maps.newHashMap();
    }
  };

}
