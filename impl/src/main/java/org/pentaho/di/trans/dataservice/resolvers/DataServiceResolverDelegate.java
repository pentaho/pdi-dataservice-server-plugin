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

package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.osgi.service.blueprint.container.ServiceUnavailableException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataServiceResolverDelegate implements DataServiceResolver {

  private List<DataServiceResolver> resolvers;

  public DataServiceResolverDelegate( List<DataServiceResolver> resolvers ) {
    this.resolvers = resolvers;
  }

  @Override public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceMetas.addAll( resolver.getDataServices( logger ) );
    }
    return dataServiceMetas;
  }

  @Override public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceMetas.addAll( resolver.getDataServices( dataServiceName, logger ) );
    }
    return dataServiceMetas;
  }

  @Override public DataServiceMeta getDataService( String dataServiceName ) {
    for ( DataServiceResolver resolver : resolvers ) {
      DataServiceMeta dataServiceMeta = resolver.getDataService( dataServiceName );
      if ( dataServiceMeta != null ) {
        return dataServiceMeta;
      }
    }
    return null;
  }

  @Override public List<String> getDataServiceNames( String dataServiceName ) {
    for ( DataServiceResolver resolver : resolvers ) {
      List<String> dataServiceNames = resolver.getDataServiceNames( dataServiceName );
      if ( dataServiceNames != null && dataServiceNames.size() > 0 ) {
        return dataServiceNames;
      }
    }
    return Collections.emptyList();
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) throws KettleException {
    boolean foundDataService = false;
    for ( DataServiceResolver resolver : resolvers ) {
      try {
        DataServiceExecutor.Builder builder = resolver.createBuilder( sql );
        if ( builder != null ) {
          return builder;
        } else {
          foundDataService = foundDataService || ( resolver.getDataService( sql.getServiceName() ) != null );
        }
      } catch ( ServiceUnavailableException ignored ) {
      }
    }
    throw new KettleException( foundDataService ? "Error when creating builder for sql query"
        : MessageFormat.format( "Data Service {0} was not found", sql.getServiceName() ) );
  }

  @Override public List<String> getDataServiceNames() {
    List<String> dataServiceNames = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceNames.addAll( resolver.getDataServiceNames() );
    }
    return dataServiceNames;
  }
}
