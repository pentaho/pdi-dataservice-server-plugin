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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
class ExecutorQueryService implements Query.Service {
  private DataServiceFactory factory;

  public ExecutorQueryService( DataServiceFactory factory ) {
    this.factory = factory;
  }

  @Override public Query prepareQuery( String sqlString, int maxRows, Map<String, String> parameters ) throws KettleException {
    SQL sql = new SQL( sqlString );
    Query query;
    try {
      DataServiceExecutor executor = factory.createBuilder( sql )
        .rowLimit( maxRows )
        .parameters( parameters )
        .build();
      query = new ExecutorQuery( executor );
    } catch ( Exception e ) {
      factory.getLogChannel().logError( "Failed to execute data service " + sql.getServiceName(), e );
      Throwables.propagateIfInstanceOf( e, KettleException.class );
      throw new KettleException( e );
    }
    return query;
  }

  public static DataOutputStream asDataOutputStream( OutputStream outputStream ) {
    return outputStream instanceof DataOutputStream ?
      ( (DataOutputStream) outputStream ) :
      new DataOutputStream( outputStream );
  }

  private static class ExecutorQuery implements Query {

    private final DataServiceExecutor executor;

    public ExecutorQuery( DataServiceExecutor executor ) {
      this.executor = executor;
    }

    public void writeTo( OutputStream outputStream ) throws IOException {
      executor.executeQuery( asDataOutputStream( outputStream ) ).waitUntilFinished();
    }

    @Override public List<Trans> getTransList() {
      return ImmutableList.of( executor.getServiceTrans(), executor.getGenTrans() );
    }

  }
}
