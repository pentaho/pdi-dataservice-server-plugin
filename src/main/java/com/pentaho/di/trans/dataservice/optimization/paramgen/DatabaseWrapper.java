/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.optimization.paramgen;

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
