/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryLocator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bmorrise on 8/30/16.
 */
public class MetaStoreResolver implements DataServiceResolver {

  private DataServiceDelegate delegate;

  public MetaStoreResolver( KettleRepositoryLocator repositoryLocator, DataServiceContext context ) {
    if ( repositoryLocator != null ) {
      this.delegate = new DataServiceDelegate( context ) {
        @Override public Repository getRepository() {
          return repositoryLocator.getRepository();
        }
      };
    } else {
      this.delegate = DataServiceDelegate.withDefaultSpoonInstance( context );
    }
  }

  @Override
  public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    return getDataServices( logger );
  }

  @Override
  public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    return (List<DataServiceMeta>) delegate.getDataServices( logger );
  }

  @Override
  public DataServiceMeta getDataService( String dataServiceName ) {
    try {
      return delegate.getDataService( dataServiceName );
    } catch ( MetaStoreException e ) {
      return null;
    }
  }

  @Override public List<String> getDataServiceNames( String dataServiceName ) {
    return getDataServiceNames();
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) {
    try {
      return delegate.createBuilder( sql );
    } catch ( MetaStoreException e ) {
      return null;
    }
  }

  @Override public List<String> getDataServiceNames() {
    try {
      return delegate.getDataServiceNames();
    } catch ( MetaStoreException e ) {
      return new ArrayList<>();
    }
  }
}
