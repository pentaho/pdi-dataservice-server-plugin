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
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.text.MessageFormat;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.pentaho.di.i18n.BaseMessages.getString;

public abstract class DataServiceFactory extends DataServiceMetaStoreUtil {
  private static final Class<?> PKG = DataServiceFactory.class;
  private static IMetaStore memoryMetaStore = new MemoryMetaStore();
  protected static IMetaStore localPentahoMetaStore = openLocalPentahoMetaStore();

  private static IMetaStore openLocalPentahoMetaStore() {
    try {
      return MetaStoreConst.openLocalPentahoMetaStore();
    } catch ( MetaStoreException e ) {
      LogChannel.GENERAL.logError( getString( PKG, "DataServiceFactory.LocalMetaStoreError" ), e );
      return null;
    }
  }

  protected DataServiceFactory( DataServiceContext context ) {
    super( context.getMetaStoreUtil() );
  }

  public abstract Repository getRepository();

  public IMetaStore getMetaStore() {
    Repository repository = getRepository();
    return checkNotNull( repository != null ? repository.getMetaStore() : localPentahoMetaStore,
      getString( PKG, "DataServiceFactory.NullMetaStore" )
    );
  }

  public void saveTransient( DataServiceMeta dataService ) throws MetaStoreException {
    getDataServiceFactory( memoryMetaStore ).saveElement( dataService );
    try {
      getServiceTransFactory( memoryMetaStore ).saveElement( ServiceTrans.create( checkDefined( dataService ) ) );
    } catch ( Exception e ) {
      throw new MetaStoreException( MessageFormat.format( "Unable to save data service {0}.", dataService.getName() ) );
    }
  }

  public DataServiceMeta getDataService( String serviceName ) throws MetaStoreException {
    try {
      DataServiceMeta dataServiceMeta = getTransientDataSerivce( serviceName );
      if ( dataServiceMeta != null ) {
        return dataServiceMeta;
      }
    } catch ( MetaStoreException e ) {
      return getDataService( serviceName, getRepository(), getMetaStore() );
    }

    return null;
  }

  public Iterable<DataServiceMeta> getDataServices( Function<Exception, Void> exceptionHandler ) {
    List<DataServiceMeta> dataServiceMetas = (List<DataServiceMeta>) getTransientDataServices( exceptionHandler );
    dataServiceMetas
      .addAll( (List<DataServiceMeta>) getDataServices( getRepository(), getMetaStore(), exceptionHandler ) );

    return dataServiceMetas;
  }

  public List<String> getDataServiceNames() throws MetaStoreException {
    List<String> dataServiceNames = getTransientDataServiceNames();
    dataServiceNames.addAll( getDataServiceNames( getMetaStore() ) );

    return dataServiceNames;
  }

  public DataServiceMeta getTransientDataSerivce( String serviceName ) throws MetaStoreException {
    return getDataService( serviceName, getRepository(), memoryMetaStore, memoryMetaStore );
  }

  public List<String> getTransientDataServiceNames() throws MetaStoreException {
    return getDataServiceNames( memoryMetaStore );
  }

  public Iterable<DataServiceMeta> getTransientDataServices( Function<Exception, Void> exceptionHandler ) {
    return getDataServices( getRepository(), memoryMetaStore, memoryMetaStore, exceptionHandler );
  }

  public DataServiceExecutor.Builder createBuilder( SQL sql ) throws MetaStoreException {
    // Locate data service and return a new builder
    DataServiceMeta dataService = getDataService( sql.getServiceName() );
    return new DataServiceExecutor.Builder( sql, dataService, context );
  }

}
