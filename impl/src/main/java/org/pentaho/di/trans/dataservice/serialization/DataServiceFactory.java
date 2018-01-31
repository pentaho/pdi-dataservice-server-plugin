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

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.pentaho.di.i18n.BaseMessages.getString;

public abstract class DataServiceFactory extends DataServiceMetaStoreUtil {
  private static final Class<?> PKG = DataServiceFactory.class;
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

  public DataServiceMeta getDataService( String serviceName ) throws MetaStoreException {
    return getDataService( serviceName, getRepository(), getMetaStore() );
  }

  public Iterable<DataServiceMeta> getDataServices( Function<Exception, Void> exceptionHandler ) {
    return getDataServices( getRepository(), getMetaStore(), exceptionHandler );
  }

  public List<String> getDataServiceNames() throws MetaStoreException {
    return getDataServiceNames( getMetaStore() );
  }

  public DataServiceExecutor.Builder createBuilder( SQL sql ) throws MetaStoreException {
    // Locate data service and return a new builder
    DataServiceMeta dataService = getDataService( sql.getServiceName() );
    return new DataServiceExecutor.Builder( sql, dataService, context );
  }

}
