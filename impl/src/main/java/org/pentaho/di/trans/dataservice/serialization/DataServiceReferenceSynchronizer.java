/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.Context;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.pentaho.metastore.util.PentahoDefaults.NAMESPACE;

/**
 * Responsible for synchronizing data service metadata embedded within a
 * transformation with the ServiceTrans entries in the metastore.
 */
public class DataServiceReferenceSynchronizer {

  private final Context context;

  public DataServiceReferenceSynchronizer( Context context ) {
    this.context = context;
  }

  @VisibleForTesting
  public  DataServiceReferenceSynchronizer() {
    this.context = null;
  }

  public void sync( TransMeta transMeta, Function<? super Exception, ?> exceptionHandler ) {
    sync( transMeta, exceptionHandler, false );
  }

  public void sync( TransMeta transMeta, Function<? super Exception, ?> exceptionHandler, boolean overwrite ) {
    MetaStoreFactory<ServiceTrans> serviceTransMetaStoreFactory =
      getMetastoreFactory( transMeta.getMetaStore(), ServiceTrans.class );

    final Map<String, DataServiceMeta> servicesInTrans;
    final Map<String, ServiceTrans> allPublished;
    try {
      servicesInTrans = getDataServicesInTransMeta( transMeta );
      allPublished = getAllPublishedServiceTrans( transMeta, serviceTransMetaStoreFactory );
    } catch ( MetaStoreException e ) {
      exceptionHandler.apply( e );
      return;
    }
    final Map<String, ServiceTrans> publishedInTrans = getPublishedServicesFromTransMeta( transMeta, allPublished );

    List<String> toDelete = publishedInTrans.keySet().stream()
      .filter( name -> !servicesInTrans.keySet().contains( name ) )
      .collect( Collectors.toList() );
    List<String> nameConflicts = servicesInTrans.keySet().stream()
      .filter( name -> allPublished.keySet().contains( name ) && !publishedInTrans.keySet().contains( name ) )
      .collect( Collectors.toList() );
    List<String> toSave = servicesInTrans.keySet().stream()
      .filter( name -> overwrite || !nameConflicts.contains( name ) )
      .collect( Collectors.toList() );

    toSave.forEach( name -> {
      try {
        serviceTransMetaStoreFactory.saveElement( ServiceTrans.create( servicesInTrans.get( name ) ) );
      } catch ( Exception e ) {
        exceptionHandler.apply( e );
      }
    } );
    toDelete.forEach( name -> {
      try {
        serviceTransMetaStoreFactory.deleteElement( name );
      } catch ( MetaStoreException e ) {
        exceptionHandler.apply( e );
      }
    } );
    nameConflicts.forEach( name ->
      exceptionHandler.apply( new DataServiceAlreadyExistsException( servicesInTrans.get( name ) ) ) );
  }

  private Map<String, ServiceTrans> getPublishedServicesFromTransMeta( TransMeta transMeta,
                                                                       Map<String, ServiceTrans> publishedServices ) {
    return publishedServices.entrySet().stream()
      .filter( entry ->
        ServiceTrans.references( transMeta )
          .stream()
          .anyMatch( reference -> entry.getValue().getReferences().contains( reference ) ) )
      .collect( Collectors.toMap( e -> e.getKey(), e -> e.getValue() ) );
  }

  private Map<String, ServiceTrans> getAllPublishedServiceTrans( TransMeta transMeta,
                                                                 MetaStoreFactory<ServiceTrans>
                                                                   serviceTransMetaStoreFactory )
    throws MetaStoreException {
    return serviceTransMetaStoreFactory.getElements().stream()
      .filter( serviceTrans -> serviceTrans.getReferences().stream()
        .anyMatch( reference -> reference.exists( transMeta.getRepository() ) ) )
      .collect( Collectors.toMap( ServiceTrans::getName, Function.identity() ) );
  }

  private Map<String, DataServiceMeta> getDataServicesInTransMeta( TransMeta transMeta )
    throws MetaStoreException {
    return getEmbeddedMetaStoreFactory( transMeta ).getElements().stream()
      .collect( Collectors.toMap( DataServiceMeta::getName, Function.identity() ) );
  }

  @VisibleForTesting
  protected <T> MetaStoreFactory<T> getMetastoreFactory(
    IMetaStore metaStore, Class<T> clazz ) {
    return new MetaStoreFactory<>( clazz, metaStore, NAMESPACE );
  }

  @VisibleForTesting
  protected MetaStoreFactory<DataServiceMeta> getEmbeddedMetaStoreFactory( TransMeta meta )
    throws MetaStoreException {
    assert context != null;
    return new EmbeddedMetaStoreFactory( meta, context.getPushDownFactories(), null );
  }
}
