/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.core.Const;
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

import static org.pentaho.metastore.util.PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION;
import static org.pentaho.metastore.util.PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME;
import static org.pentaho.metastore.util.PentahoDefaults.NAMESPACE;

public class DataServiceMetaStoreUtil {
  public static final String GROUP_DATA_SERVICE = "DataService";
  public static final String DATA_SERVICE_NAME = "name";
  public static final String DATA_SERVICE_STEPNAME = "stepname";

  private final List<PushDownFactory> pushDownFactories;

  public DataServiceMetaStoreUtil( List<PushDownFactory> pushDownFactories ) {
    this.pushDownFactories = pushDownFactories;
  }

  public DataServiceMeta fromTransMeta( TransMeta transMeta, IMetaStore metaStore ) throws MetaStoreException {
    String serviceName = transMeta.getAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_NAME );
    if ( Const.isEmpty( serviceName ) ) {
      return null;
    }
    return getMetaStoreFactory( metaStore ).loadElement( serviceName );
  }

  public void toTransMeta( TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService )
    throws MetaStoreException {

    if ( dataService != null && dataService.isDefined() ) {
      // Also make sure the metastore object is stored properly
      //
      transMeta.setAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_NAME, dataService.getName() );
      transMeta.setAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_STEPNAME, dataService.getStepname() );

      // Initialize optimizations
      for ( PushDownOptimizationMeta optMeta : dataService.getPushDownOptimizationMeta() ) {
        optMeta.getType().init( transMeta, dataService, optMeta );
      }

      LogChannel.GENERAL.logBasic( "Saving data service in meta store '" + transMeta.getMetaStore() + "'" );

      // Leave trace of this transformation...
      //
      Repository repository = transMeta.getRepository();
      if ( transMeta.getRepository() != null ) {
        dataService.setTransRepositoryPath(
          transMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta
            .getName() );
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
    }
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
