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


package org.pentaho.di.trans.dataservice.clients;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.util.List;
import java.util.Map;

/**
 * Created by bmorrise on 8/31/16.
 */
public class QueryServiceDelegate implements Query.Service {

  private List<Query.Service> queryServices;

  public QueryServiceDelegate( List<Query.Service> queryServices ) {
    this.queryServices = queryServices;
  }

  @Override public Query prepareQuery( String sql, int maxRows, Map<String, String> parameters )
    throws KettleException {
    for ( Query.Service queryService : queryServices ) {
      Query query = queryService.prepareQuery( sql, maxRows, parameters );
      if ( query != null ) {
        return query;
      }
    }
    return null;
  }

  @Override public Query prepareQuery( String sql, IDataServiceClientService.StreamingMode windowMode,
                                       long windowSize, long windowEvery, long windowLimit,
                                       final Map<String, String> parameters ) throws KettleException {
    for ( Query.Service queryService : queryServices ) {
      Query query = queryService.prepareQuery( sql, windowMode, windowSize, windowEvery, windowLimit, parameters );
      if ( query != null ) {
        return query;
      }
    }
    return null;
  }
}
