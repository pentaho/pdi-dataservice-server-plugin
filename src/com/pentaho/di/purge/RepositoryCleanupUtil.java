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

package com.pentaho.di.purge;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.pentaho.platform.security.policy.rolebased.actions.AdministerSecurityAction;
import org.pentaho.platform.util.RepositoryPathEncoder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;

/*
 * java RepositoryCleanupUtil url=http://localhost:9080 user=admin password=password path=/home versionCount=9 fileFilter=* purgeFiles=false purgeRevisions=false purgeSharedObjects=false deleteBeforeDate=1/1/1900 logLevel=OFF
 */

public class RepositoryCleanupUtil {

  private Client client = null;

  // Utility parameters.
  private final String URL = "url";
  private final String USER = "user";
  private final String PASS = "password";
  private final String PATH = "path";
  private final String VER_COUNT = "versionCount";
  private final String DEL_DATE = "deleteBeforeDate";
  private final String FILE_FILTER = "fileFilter";
  private final String PURGE_FILES = "purgeFiles";
  private final String PURGE_REV = "purgeRevisions";
  private final String PURGE_SHARED = "purgeSharedObjects";
  private final String LOG_LEVEL = "logLevel";

  // Constants.
  private final String SERVICE_NAME = "/purge";
  private final String BASE_PATH = "/pentaho-di/plugin/pur-repository-plugin/api/purge";
  private final String AUTHENTICATION = "/pentaho-di/api/authorization/action/isauthorized?authAction=";
  private final String deleteBeforeDateFormat = "MM/dd/yyyy";

  // Class properties.
  private String url = null;
  private String path = null;
  private String user = null;
  private String password = null;
  private int verCount = -1;
  private String delFrom = null;
  private String fileFilter = null;
  private String logLevel = null;
  private boolean purgeFiles = false;
  private boolean purgeRev = false;
  private boolean purgeShared = false;

  public static void main( String[] args ) {
    new RepositoryCleanupUtil( args );
  }

  public RepositoryCleanupUtil( String[] args ) {
    try {
      Map<String, String> parameters = parseParameters( args );

      validateParameters( parameters );
      authenticateLoginCredentials( user, password, url );
      String serviceURL = createServiceURL();

      System.out.println( serviceURL );

      WebResource resource = client.resource( serviceURL );
      Builder builder = resource.type( MediaType.APPLICATION_JSON ).type( MediaType.TEXT_XML_TYPE );
      ClientResponse response = builder.get( ClientResponse.class );
      if ( response != null && response.getStatus() == 200 ) {
        System.out.println( "Operation completed successfully..." );
      } else {
        System.out.println( "Error while executing the operation..." );
      }

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
      throw new Exception( "\n\nErrors:\nError when parsing parameters.\n" );
    }
    if ( arguments.size() == 0 ) {
      throw new Exception( printHelp() );
    }
    return arguments;
  }

  private void validateParameters( Map<String, String> arguments ) throws Exception {
    String aUrl = arguments.get( URL );
    String aPath = arguments.get( PATH );
    String aUser = arguments.get( USER );
    String aPassword = arguments.get( PASS );
    String aVerCount = arguments.get( VER_COUNT );
    String aDelFrom = arguments.get( DEL_DATE );
    String aPurgeFiles = arguments.get( PURGE_FILES );
    String aPurgeRev = arguments.get( PURGE_REV );
    String aPurgeShared = arguments.get( PURGE_SHARED );
    String aLogLevel = arguments.get( LOG_LEVEL );
    String aFileFilter = arguments.get( FILE_FILTER );

    StringBuffer errors = new StringBuffer();

    // TODO validation pending...
    fileFilter = aFileFilter;

    if ( aLogLevel != null
        && !( aLogLevel.equals( "ALL" ) || aLogLevel.equals( "DEBUG" ) || aLogLevel.equals( "ERROR" )
            || aLogLevel.equals( "FATAL" ) || aLogLevel.equals( "INFO" ) || aLogLevel.equals( "OFF" )
            || aLogLevel.equals( "TRACE" ) || aLogLevel.equals( "WARN" ) ) ) {
      errors.append( "-" + LOG_LEVEL + "=" + aLogLevel
          + " valid values are ALL,DEBUG,ERROR,FATAL,TRACE,INFO,OFF,TRACE,WARN.\n" );
    } else {
      logLevel = aLogLevel;
    }

    if ( aUrl == null ) {
      errors.append( "-" + URL + " parameter is missing.\n" );
    } else {
      url = aUrl;
    }

    if ( aPath == null ) {
      errors.append( "-" + PATH + " parameter is missing.\n" );
    } else {
      path = aPath;
    }

    if ( aUser == null ) {
      errors.append( "-" + USER + " parameter is missing.\n" );
    } else {
      user = aUser;
    }

    if ( aPassword == null ) {
      errors.append( "-" + PASS + " parameter is missing.\n" );
    } else {
      password = aPassword;
    }

    if ( aPurgeFiles != null && !( aPurgeFiles.equalsIgnoreCase( "true" ) || aPurgeFiles.equalsIgnoreCase( "false" ) ) ) {
      errors.append( "-" + PURGE_FILES + "=" + aPurgeFiles + " should be true or false.\n" );
    } else {
      purgeFiles = Boolean.parseBoolean( aPurgeFiles );
    }

    if ( aPurgeRev != null && !( aPurgeRev.equalsIgnoreCase( "true" ) || aPurgeRev.equalsIgnoreCase( "false" ) ) ) {
      errors.append( "-" + PURGE_REV + "=" + aPurgeRev + " should be true or false.\n" );
    } else {
      purgeRev = Boolean.parseBoolean( aPurgeRev );
    }

    if ( aPurgeShared != null
        && !( aPurgeShared.equalsIgnoreCase( "true" ) || aPurgeShared.equalsIgnoreCase( "false" ) ) ) {
      errors.append( "-" + PURGE_SHARED + "=" + aPurgeShared + " should be true or false.\n" );
    } else {
      purgeShared = Boolean.parseBoolean( aPurgeShared );
    }

    SimpleDateFormat sdf = new SimpleDateFormat( deleteBeforeDateFormat );
    sdf.setLenient( false );
    try {
      sdf.parse( aDelFrom );
      delFrom = aDelFrom;
    } catch ( ParseException e ) {
      errors
          .append( "-" + DEL_DATE + "=" + aDelFrom + " should be defined in " + deleteBeforeDateFormat + " format.\n" );
    }

    try {
      verCount = Integer.parseInt( aVerCount );
    } catch ( NumberFormatException e ) {
      errors.append( "-" + VER_COUNT + "=" + aVerCount + " should be an integer.\n" );
    }

    if ( errors.length() != 0 ) {
      errors.insert( 0, "\n\nErrors:\n" );
      throw new Exception( errors.toString() );
    }
  }

  private void authenticateLoginCredentials( String username, String password, String url ) throws Exception {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put( JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE );
    client = Client.create( clientConfig );
    client.addFilter( new HTTPBasicAuthFilter( username, password ) );

    WebResource resource = client.resource( url + AUTHENTICATION + AdministerSecurityAction.NAME );
    String response = resource.get( String.class );

    if ( !response.equals( "true" ) ) {
      throw new Exception( "Access Denied..." );
    }
  }

  private String createServiceURL() throws Exception {

    StringBuffer service = new StringBuffer();
    service.append( url );
    service.append( BASE_PATH );

    service.append( "/" );
    path = RepositoryPathEncoder.encodeRepositoryPath( path );
    path = RepositoryPathEncoder.encode( path );
    service.append( path );

    service.append( SERVICE_NAME );
    service.append( "?" );

    if ( verCount != -1 && !purgeRev ) {
      service.append( "&" );
      service.append( VER_COUNT );
      service.append( "=" );
      service.append( verCount );
    }

    if ( delFrom != null && !purgeRev ) {
      service.append( "&" );
      service.append( DEL_DATE );
      service.append( "=" );
      service.append( delFrom );
    }

    if ( fileFilter != null ) {
      service.append( "&" );
      service.append( FILE_FILTER );
      service.append( "=" );
      service.append( fileFilter );
    }

    if ( logLevel != null ) {
      service.append( "&" );
      service.append( LOG_LEVEL );
      service.append( "=" );
      service.append( logLevel );
    }

    if ( purgeFiles ) {
      service.append( "&" );
      service.append( PURGE_FILES );
      service.append( "=" );
      service.append( purgeFiles );
    }

    if ( purgeRev ) {
      service.append( "&" );
      service.append( PURGE_REV );
      service.append( "=" );
      service.append( purgeRev );
    }

    if ( purgeShared ) {
      service.append( "&" );
      service.append( PURGE_SHARED );
      service.append( "=" );
      service.append( purgeShared );
    }

    return service.toString();
  }

  private String printHelp() {

    // TODO improve this help description....
    StringBuffer help = new StringBuffer();

    help.append( "\n\nSome of the following parameters must be provided:\n" );

    help.append( "-'" + URL
        + "' Is mandatory, should be a valid parameter where the Data Integration Server resides, ie " + URL
        + "=http://localhost:9080\n" );
    help.append( "-'" + USER + "' Is mandatory\n" );
    help.append( "-'" + PASS + "' Is mandatory\n" );
    help.append( "-'" + PATH + "' Is mandatory\n" );
    help.append( "-'" + VER_COUNT + "' should be an integer, ie " + VER_COUNT + "=2\n" );
    help.append( "-'" + DEL_DATE + "' should be a valid date in the following format " + deleteBeforeDateFormat
        + ", ie " + DEL_DATE + "=12/01/2014\n" );
    help.append( "-'" + FILE_FILTER + "', ie " + FILE_FILTER + "=*.ktr | *.kjb\n" );
    help.append( "-'" + PURGE_FILES + "' should be true or false, ie " + PURGE_FILES + "=true\n" );
    help.append( "-'" + PURGE_REV + "' should be true or false, ie " + PURGE_REV + "=true\n" );
    help.append( "-'" + PURGE_SHARED + "' should be true or false, ie " + PURGE_SHARED + "=true\n" );
    help.append( "-'" + LOG_LEVEL + "' valid values are ALL,DEBUG,ERROR,FATAL,TRACE,INFO,OFF,TRACE,WARN, ie "
        + LOG_LEVEL + "=ALL\n" );

    return help.toString();
  }
}
