/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.metastore.api.IMetaStore;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class DataServiceClient implements IDataServiceClientService {
  private final Query.Service queryService;
  private final DataServiceResolver resolver;
  private final ExecutorService executorService;
  private LogChannelInterface log;

  public DataServiceClient( Query.Service queryService, DataServiceResolver resolver,
                            ExecutorService executorService ) {
    this.queryService = queryService;
    this.resolver = resolver;
    this.executorService = executorService;
  }

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {
    try {
      // Create a pipe to for results
      SafePipedStreams pipe = new SafePipedStreams();

      // Prepare query, exception will be thrown if query is invalid
      Query query = prepareQuery( sqlQuery, maxRows, ImmutableMap.of() );

      // Write query results to pipe on a separate thread
      executorService.execute( () -> {
        try ( OutputStream out = pipe.out ) {
          // Write out results
          query.writeTo( out );
          // Pipe will automatically close on this end
        } catch ( Exception e ) {
          log.logError( e.getMessage(), e );
        }
      } );

      return new DataInputStream( pipe.in );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }
  }

  public Query prepareQuery( String sql, int maxRows, Map<String, String> parameters )
    throws KettleException {
    Query query = queryService.prepareQuery( sql, maxRows, parameters );
    if ( query != null ) {
      return query;
    }
    throw new KettleException( "Unable to resolve query: " + sql );
  }

  @Override public List<IThinServiceInformation> getServiceInformation() throws SQLException {
    List<IThinServiceInformation> services = Lists.newArrayList();

    for ( DataServiceMeta service : resolver.getDataServices( logErrors()::apply ) ) {
      TransMeta transMeta = service.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        IThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      } catch ( Exception e ) {
        String message = MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          service.getName(), transMeta.getName() );
        log.logError( message, e );
      }
    }

    return services;
  }

  @Override public ThinServiceInformation getServiceInformation( String name ) throws SQLException {

    DataServiceMeta dataServiceMeta = resolver.getDataService( name );

    if ( dataServiceMeta != null ) {
      TransMeta transMeta = dataServiceMeta.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( dataServiceMeta.getStepname() );
        return new ThinServiceInformation( dataServiceMeta.getName(), serviceFields );
      } catch ( Exception e ) {
        String message = MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          dataServiceMeta.getName(), transMeta.getName() );
        log.logError( message, e );
      }
    }

    return null;
  }

  @Override public List<String> getServiceNames( String serviceName ) throws SQLException {
    return resolver.getDataServiceNames( serviceName );
  }

  @Override public List<String> getServiceNames() throws SQLException {
    return resolver.getDataServiceNames();
  }


  private Function<Exception, Void> logErrors() {
    return e -> {
      getLogChannel().logError( "Unable to retrieve data service", e );
      return null;
    };
  }

  public void setLogChannel( LogChannelInterface log ) {
    this.log = log;
  }

  public LogChannelInterface getLogChannel() {
    return log;
  }

  /**
   * @deprecated Property is unused. See {@link IDataServiceClientService#setRepository(Repository)}
   */
  @Deprecated
  public void setRepository( Repository repository ) {
  }

  /**
   * @deprecated Property is unused. See {@link IDataServiceClientService#setMetaStore(IMetaStore)}
   */
  @Deprecated
  public void setMetaStore( IMetaStore metaStore ) {
  }

  /**
   * Holds a pair of piped input and output streams.
   * <p>
   * The streams are "safe" in that writing into the pipe after the reading end has closed will not result in an
   * exception. Data written while the read end is closed will be ignored.
   *
   * @author hudak
   */
  private static class SafePipedStreams {
    final PipedOutputStream out;
    final PipedInputStream in;
    private volatile boolean open = true;

    private SafePipedStreams() throws IOException {
      in = new PipedInputStream() {
        @Override public void close() throws IOException {
          ifOpen( () -> open = false );
          super.close();
        }
      };
      out = new PipedOutputStream( in ) {
        @Override public void write( int b ) throws IOException {
          ifOpen( () -> super.write( b ) );
        }

        @Override public void write( byte[] b, int off, int len ) throws IOException {
          ifOpen( () -> super.write( b, off, len ) );
        }
      };
    }

    private synchronized void ifOpen( IOExceptionAction action ) throws IOException {
      if ( open ) {
        action.call();
      }
    }

    private interface IOExceptionAction {
      void call() throws IOException;
    }
  }
}
