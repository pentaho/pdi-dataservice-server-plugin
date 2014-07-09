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

package com.pentaho.di.revision;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

import org.pentaho.platform.util.RepositoryPathEncoder;

public class RepositoryCleanupUtil {

  // Parameters.
  private final String URL = "url"; // URL of the EE repository.
  private final String REP = "rep"; // The name of the repository.
  private final String USER = "user"; // The user name for the repository.
  private final String PASS = "password"; // The password for the repository.
  private final String PATH = "path"; // Path to be cleaned. Operation will be performed recursively from here.
  private final String KEEP = "numOfVerToKeep"; // The number of versions to keep.
  private final String DEL_DATE = "deleteBeforeDate"; // Date to delete versions from.

  // REST service constants.
  private final String BASE_PATH = "/pdi-ee/api/version";
  private final String SERVICE_NAME = "/delete";

  public static void main( String[] args ) {
    new RepositoryCleanupUtil( args );
  }

  public RepositoryCleanupUtil( String[] args ) {
    try {
      Map<String, String> parameters = parseParameters( args );
      String serviceURL = createServiceURL( parameters );
      System.out.println( serviceURL );
      URLConnection connection = new URL( serviceURL ).openConnection();
      connection.setDoOutput( true );
      BufferedReader in = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
      String inputLine;
      while ( ( inputLine = in.readLine() ) != null ) {
        System.out.println( inputLine );
      }
      in.close();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  private Map<String, String> parseParameters( String[] args ) throws Exception {
    Map<String, String> arguments = new HashMap();
    try {
      for ( String arg : args ) {
        String param = arg.substring( 0, arg.indexOf( "=" ) );
        String value = arg.substring( arg.indexOf( "=" ) + 1, arg.length() );
        arguments.put( param, value );
      }
    } catch ( Exception e ) {
      throw new Exception( "Error when parsing parameters." );
    }
    if ( arguments.size() == 0 ) {
      throw new Exception( "Must provide parameters." );
    }
    return arguments;
  }

  private String createServiceURL( Map<String, String> arguments ) throws Exception {

    String url = arguments.get( URL );
    String path = arguments.get( PATH );
    String rep = arguments.get( REP );
    String user = arguments.get( USER );
    String password = arguments.get( PASS );
    String keep = arguments.get( KEEP );
    String del_from = arguments.get( DEL_DATE );

    if ( url == null ) {
      throw new Exception( URL + " parameter is missing." );
    }
    if ( path == null ) {
      throw new Exception( PATH + " parameter is missing." );
    }
    if ( rep == null ) {
      throw new Exception( REP + " parameter is missing." );
    }
    if ( user == null ) {
      throw new Exception( USER + " parameter is missing." );
    }
    if ( password == null ) {
      throw new Exception( PASS + " parameter is missing." );
    }
    if ( keep == null && del_from == null ) {
      throw new Exception( KEEP + " or " + DEL_DATE + " parameter is missing." );
    }

    StringBuffer service = new StringBuffer();
    service.append( url );
    service.append( BASE_PATH );

    service.append( "/" );
    path = RepositoryPathEncoder.encodeRepositoryPath( path );
    path = RepositoryPathEncoder.encode( path );
    service.append( path );

    service.append( SERVICE_NAME );

    service.append( "?" );
    service.append( REP );
    service.append( "=" );
    service.append( rep );

    service.append( "&" );
    service.append( USER );
    service.append( "=" );
    service.append( user );

    service.append( "&" );
    service.append( PASS );
    service.append( "=" );
    service.append( password );

    service.append( "&" );
    service.append( keep != null ? KEEP : DEL_DATE );
    service.append( "=" );
    service.append( keep != null ? keep : del_from );

    return service.toString();
  }
}
