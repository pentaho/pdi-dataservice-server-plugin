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

import java.io.File;
import java.io.FileOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.pentaho.platform.security.policy.rolebased.actions.AdministerSecurityAction;
import org.pentaho.platform.util.RepositoryPathEncoder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.multipart.FormDataMultiPart;

public class RepositoryCleanupUtil {

  private Client client = null;

  // Utility parameters.
  private final String URL = "url";
  private final String USER = "user";
  private final String PASS = "password";
  private final String VER_COUNT = "versionCount";
  private final String DEL_DATE = "deleteBeforeDate";
  private final String FILE_FILTER = "fileFilter";
  private final String PURGE_FILES = "purgeFiles";
  private final String PURGE_REV = "purgeRevisions";
  private final String PURGE_SHARED = "purgeSharedObjects";
  private final String LOG_LEVEL = "logLevel";

  // Constants.
  private final String SERVICE_NAME = "purge";
  private final String BASE_PATH = "/pentaho-di/plugin/pur-repository-plugin/api/purge";
  private final String AUTHENTICATION = "/pentaho-di/api/authorization/action/isauthorized?authAction=";
  private final String deleteBeforeDateFormat = "MM/dd/yyyy";

  // Class properties.
  private String url = null;
  private String username = null;
  private String password = null;
  private int verCount = -1;
  private String delFrom = null;
  private String logLevel = null;
  private boolean purgeFiles = false;
  private boolean purgeRev = false;
  private boolean purgeShared = false;
  private String fileFilter = "*.ktr|*.kjb";
  private String repositoryPath = "/";

  public static void main( String[] args ) {
    new RepositoryCleanupUtil().purge( args );
  }

  public void purge( String[] options ) {
    FormDataMultiPart form = null;
    try {
      Map<String, String> parameters = parseExecutionOptions( options );
      validateParameters( parameters );
      authenticateLoginCredentials();

      String serviceURL = createPurgeServiceURL();
      form = createParametersForm();

      WebResource resource = client.resource( serviceURL );
      ClientResponse response = resource.type( MediaType.MULTIPART_FORM_DATA ).post( ClientResponse.class, form );

      if ( response != null && response.getStatus() == 200 ) {
        String log = response.getEntity( String.class );
        String logName = writeLog( log );
        System.out.println( "Operation completed successfully, '" + logName + "' generated." );
      } else {
        System.out.println( "Error while executing the operation..." );
      }

    } catch ( Exception e ) {
      e.printStackTrace();
    } finally {
      client.destroy();
      form.cleanup();
    }
  }

  private Map<String, String> parseExecutionOptions( String[] args ) throws Exception {
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
    String aUser = arguments.get( USER );
    String aPassword = arguments.get( PASS );
    String aVerCount = arguments.get( VER_COUNT );
    String aDelFrom = arguments.get( DEL_DATE );
    String aPurgeFiles = arguments.get( PURGE_FILES );
    String aPurgeRev = arguments.get( PURGE_REV );
    String aPurgeShared = arguments.get( PURGE_SHARED );
    String aLogLevel = arguments.get( LOG_LEVEL );

    StringBuffer errors = new StringBuffer();

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

    if ( aUser == null ) {
      errors.append( "-" + USER + " parameter is missing.\n" );
    } else {
      username = aUser;
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

    if ( aDelFrom != null ) {
      SimpleDateFormat sdf = new SimpleDateFormat( deleteBeforeDateFormat );
      sdf.setLenient( false );
      try {
        sdf.parse( aDelFrom );
        delFrom = aDelFrom;
      } catch ( ParseException e ) {
        errors.append( "-" + DEL_DATE + "=" + aDelFrom + " should be defined in " + deleteBeforeDateFormat
            + " format.\n" );
      }
    }

    if ( aVerCount != null ) {
      try {
        verCount = Integer.parseInt( aVerCount );
      } catch ( NumberFormatException e ) {
        errors.append( "-" + VER_COUNT + "=" + aVerCount + " should be an integer.\n" );
      }
    }

    if ( errors.length() != 0 ) {
      errors.insert( 0, "\n\nErrors:\n" );
      throw new Exception( errors.toString() );
    }
  }

  private void authenticateLoginCredentials() throws Exception {
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

  private String createPurgeServiceURL() throws Exception {
    StringBuffer service = new StringBuffer();
    service.append( url );
    service.append( BASE_PATH );
    service.append( "/" );

    String path = RepositoryPathEncoder.encodeRepositoryPath( repositoryPath );
    path = RepositoryPathEncoder.encode( path );
    service.append( path + "/" );

    service.append( SERVICE_NAME );
    return service.toString();
  }

  private FormDataMultiPart createParametersForm() {
    FormDataMultiPart form = new FormDataMultiPart();
    if ( verCount != -1 && !purgeRev ) {
      form.field( VER_COUNT, Integer.toString( verCount ), MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( delFrom != null && !purgeRev ) {
      form.field( DEL_DATE, delFrom, MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( fileFilter != null ) {
      form.field( FILE_FILTER, fileFilter, MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( logLevel != null ) {
      form.field( LOG_LEVEL, logLevel, MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( purgeFiles ) {
      form.field( PURGE_FILES, Boolean.toString( purgeFiles ), MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( purgeRev ) {
      form.field( PURGE_REV, Boolean.toString( purgeRev ), MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    if ( purgeShared ) {
      form.field( PURGE_SHARED, Boolean.toString( purgeShared ), MediaType.MULTIPART_FORM_DATA_TYPE );
    }
    return form;
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
    help.append( "-'" + VER_COUNT + "' should be an integer, ie " + VER_COUNT + "=2\n" );
    help.append( "-'" + DEL_DATE + "' should be a valid date in the following format " + deleteBeforeDateFormat
        + ", ie " + DEL_DATE + "=12/01/2014\n" );
    help.append( "-'" + PURGE_FILES + "' should be true or false, ie " + PURGE_FILES + "=true\n" );
    help.append( "-'" + PURGE_REV + "' should be true or false, ie " + PURGE_REV + "=true\n" );
    help.append( "-'" + PURGE_SHARED + "' should be true or false, ie " + PURGE_SHARED + "=true\n" );
    help.append( "-'" + LOG_LEVEL + "' valid values are ALL,DEBUG,ERROR,FATAL,TRACE,INFO,OFF,TRACE,WARN, ie "
        + LOG_LEVEL + "=ALL\n" );

    return help.toString();
  }

  private String writeLog( String message ) throws Exception {
    String logName = "purge-utility-log-" + new Date() + ".txt";
    File file = new File( logName );
    FileOutputStream fout = FileUtils.openOutputStream( file );
    IOUtils.copy( IOUtils.toInputStream( message ), fout );
    fout.close();
    return logName;
  }
}
