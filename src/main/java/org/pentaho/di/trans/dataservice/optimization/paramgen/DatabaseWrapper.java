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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
* @author nhudak
*/
class DatabaseWrapper extends Database {
  Map<String, RuntimePushDown> pushDownMap = new HashMap<String, RuntimePushDown>();
  private final LogChannelInterface log;

  public DatabaseWrapper( Database db ) {
    super( db, db.getDatabaseMeta() );

    log = new LogChannel( this, db );

    shareVariablesWith( db );

    setConnectionGroup( db.getConnectionGroup() );
    setPartitionId( db.getPartitionId() );
  }

  public synchronized void connect() throws KettleDatabaseException {
    super.connect( getConnectionGroup(), getPartitionId() );
  }

  @Override
  public ResultSet openQuery( String sql, RowMetaInterface paramsMeta, Object[] data, int fetch_mode, boolean lazyConversion ) throws KettleDatabaseException {
    List<Object> params = data == null ? new ArrayList<Object>() : new ArrayList<Object>( Arrays.asList( data ) );
    paramsMeta = paramsMeta == null ? new RowMeta() : paramsMeta;
    sql = injectRuntime( pushDownMap, sql, paramsMeta, params );
    if ( params.size() > 0 && log.isDetailed() ) {
      log.logDetailed( parameterizedQueryToString( sql, params ) );
    }
    return super.openQuery( sql, paramsMeta, params.toArray(), fetch_mode, lazyConversion );
  }

  protected String parameterizedQueryToString( String sql, List<Object> params ) {
    return String.format( "Parameterized SQL:  %s   %s", sql,  paramsToString( params ) );
  }

  private String paramsToString( List<Object> params ) {
    StringBuilder paramStr = new StringBuilder();
    paramStr.append( "{" );
    for ( int i = 1; i <= params.size(); i++ ) {
      paramStr.append( i )
        .append( ": " )
        .append( params.get( i - 1 ) );
      if ( i < params.size() ) {
        paramStr.append( ", " );
      }
    }
    return paramStr.append( "}" ).toString();
  }

  protected String injectRuntime( Map<String, RuntimePushDown> runtimeMap, String sql, final RowMetaInterface paramsMeta, final List<Object> params ) {
    if ( !params.isEmpty() ) {
      //TODO Don't know what to do if query already has parameters. Give up on optimization? Try to mix params?
      return sql;
    }

    // Determine fragment ordering and position
    StringBuilder sqlBuilder = new StringBuilder( sql );
    SortedMap<Integer, RuntimePushDown> sortedMap = new TreeMap<Integer, RuntimePushDown>();
    for ( String fragmentId : runtimeMap.keySet() ) {
      int pos = sqlBuilder.indexOf( fragmentId );
      if ( pos >= 0 ) {
        sortedMap.put( pos, runtimeMap.get( fragmentId ) );
      }
    }
    for ( RuntimePushDown runtime : sortedMap.values() ) {
      // Inject SQL fragment
      int start = sqlBuilder.indexOf( runtime.fragmentId );
      int end = start + runtime.fragmentId.length();
      sqlBuilder.replace( start, end, runtime.sqlFragment );

      // Append prepared statement parameters and meta
      paramsMeta.addRowMeta( runtime.paramsMeta );
      params.addAll( runtime.params );
    }

    return sqlBuilder.toString();
  }

  public String createRuntimePushDown( String sqlFragment, RowMeta paramsMeta, List<Object> params, String defaultValue ) {
    // Create a unique ID for this substitution, including a default value
    String fragmentId = "/*" + UUID.randomUUID().toString() + "*/" + defaultValue;

    RuntimePushDown runtimePushDown = new RuntimePushDown( fragmentId, sqlFragment, paramsMeta, params );
    pushDownMap.put( fragmentId, runtimePushDown );

    return fragmentId;
  }

  static class RuntimePushDown {
    final String fragmentId;
    final String sqlFragment;
    final RowMeta paramsMeta;
    final List<Object> params;

    public RuntimePushDown( String fragmentId, String sqlFragment, RowMeta paramsMeta, List<Object> params ) {
      this.fragmentId = fragmentId;
      this.sqlFragment = sqlFragment;
      this.paramsMeta = paramsMeta;
      this.params = params;
    }
  }
}
