/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.clients;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.CommandExecutor;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bmorrise on 12/9/15.
 */
public class CommandQueryService implements Query.Service {

  private DataServiceContext context;

  public CommandQueryService( DataServiceContext context ) {
    this.context = context;
  }

  @Override public Query prepareQuery( String sql, int maxRows, Map<String, String> parameters )
      throws KettleException {
    return prepareQuery( sql, null, 0, 0, 0, parameters );
  }

  @Override public Query prepareQuery( String sql, IDataServiceClientService.StreamingMode windowMode,
                                       long windowSize, long windowEvery, long windowLimit,
                                       final Map<String, String> parameters )
    throws KettleException {

    if ( sql.startsWith( CommandExecutor.COMMAND_START ) ) {
      return new CommandQuery( new CommandExecutor.Builder( sql, context ).build() );
    }

    return null;
  }

  public static DataOutputStream asDataOutputStream( OutputStream outputStream ) {
    return outputStream instanceof DataOutputStream
        ? ( (DataOutputStream) outputStream )
        : new DataOutputStream( outputStream );
  }

  private class CommandQuery implements Query {

    private final CommandExecutor executor;

    public CommandQuery( CommandExecutor executor ) {
      this.executor = executor;
    }

    @Override public void writeTo( OutputStream outputStream ) throws IOException {
      executor.execute( asDataOutputStream( outputStream ) );
    }

    @Override public List<Trans> getTransList() {
      return new ArrayList<>();
    }
  }
}
