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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
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
  private final String PURGE_FILES = "purgeFiles";
  private final String PURGE_REV = "purgeRevisions";
  private final String CONTENT_TARGET = "contentTarget";
  private final String LOG_LEVEL = "logLevel";
  private final String LOG_FILE = "logFile";

  //parameters in rest call that are not in command line
  private final String FILE_FILTER = "fileFilter";
  private final String PURGE_SHARED = "purgeSharedObjects";
  
  // Constants.
  private final String SERVICE_NAME = "purge";
  private final String BASE_PATH = "/pentaho-di/plugin/pur-repository-plugin/api/purge";
  private final String AUTHENTICATION = "/pentaho-di/api/authorization/action/isauthorized?authAction=";
  private final String deleteBeforeDateFormat = "MM/dd/yyyy";
  private final String OPTION_PREFIX = "-";
  private final String NEW_LINE = "\n";

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
  private String logFile = null;
  private String fileFilter;
  private String repositoryPath;
  
  public static void main( String[] args ) {
    try {
      new RepositoryCleanupUtil().purge( args );
    } catch ( Exception e ) {
      writeOut( e );
    }
    System.exit( 0 );
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
        String resultLog = response.getEntity( String.class );
        String logName = writeLog( resultLog );
        writeOut( "Operation completed successfully, '" + logName + "' generated.", false );
      } else {
        writeOut( "Error while executing the operation..." , true );
      }

    } catch ( Exception e ) {
      e.printStackTrace();
    } finally {
      client.destroy();
      form.cleanup();
    }
  }

  private Map<String, String> parseExecutionOptions( String[] args ) throws Exception {
    Map<String, String> arguments = new HashMap<String, String>();
    try {
      for ( String arg : args ) {
        String param = arg.substring( 0, arg.indexOf( "=" ) );
        String value = arg.substring( arg.indexOf( "=" ) + 1, arg.length() );
        arguments.put( param, value );
      }
    } catch ( Exception e ) {
      writeOut( "\n\nErrors:\nError when parsing parameters.\n", true );
    }
    if ( arguments.size() == 0 ) {
      writeOut( printHelp(), true );
    }
    return arguments;
  }

  private void validateParameters( Map<String, String> arguments ) throws Exception {
    String aUrl = arguments.get( OPTION_PREFIX + URL );
    String aUser = arguments.get( OPTION_PREFIX + USER );
    String aPassword = arguments.get( OPTION_PREFIX + PASS );
    String aVerCount = arguments.get( OPTION_PREFIX + VER_COUNT );
    String aDelFrom = arguments.get( OPTION_PREFIX + DEL_DATE );
    String aPurgeFiles = arguments.get( OPTION_PREFIX + PURGE_FILES );
    String aPurgeRev = arguments.get( OPTION_PREFIX + PURGE_REV );
    String aContentTarget = arguments.get( OPTION_PREFIX + CONTENT_TARGET );
    String aLogLevel = arguments.get( OPTION_PREFIX + LOG_LEVEL );
    String aLogFile = arguments.get( OPTION_PREFIX + LOG_FILE );

    StringBuffer errors = new StringBuffer();

    if ( aLogLevel != null
        && !(  aLogLevel.equals( "DEBUG" ) || aLogLevel.equals( "ERROR" )
            || aLogLevel.equals( "FATAL" ) || aLogLevel.equals( "INFO" ) || aLogLevel.equals( "OFF" )
            || aLogLevel.equals( "TRACE" ) || aLogLevel.equals( "WARN" ) ) ) {
      errors.append( OPTION_PREFIX + LOG_LEVEL + "=" + aLogLevel
          + " valid values are ALL,DEBUG,ERROR,FATAL,TRACE,INFO,OFF,TRACE,WARN.\n" );
    } else {
      logLevel = aLogLevel;
    }
    
    if ( aLogFile != null ) {
      File f = new File( aLogFile );
      if ( f.exists() && f.isDirectory() ) {
        errors.append( OPTION_PREFIX + LOG_FILE + " already exists and is a folder.\n" );
      }
      logFile = aLogFile;
    }

    if ( aUrl == null ) {
      errors.append( OPTION_PREFIX + URL + " parameter is missing.\n" );
    } else {
      url = aUrl;
    }

    if ( aUser == null ) {
      errors.append( OPTION_PREFIX + USER + " parameter is missing.\n" );
    } else {
      username = aUser;
    }

    if ( aPassword == null ) {
      errors.append( OPTION_PREFIX + PASS + " parameter is missing.\n" );
    } else {
      password = aPassword;
    }

    if ( aPurgeFiles != null && !( aPurgeFiles.equalsIgnoreCase( "true" ) || aPurgeFiles.equalsIgnoreCase( "false" ) ) ) {
      errors.append( OPTION_PREFIX + PURGE_FILES + "=" + aPurgeFiles + " should be true or false.\n" );
    } else {
      purgeFiles = Boolean.parseBoolean( aPurgeFiles );
    }

    if ( aPurgeRev != null && !( aPurgeRev.equalsIgnoreCase( "true" ) || aPurgeRev.equalsIgnoreCase( "false" ) ) ) {
      errors.append( OPTION_PREFIX + PURGE_REV + "=" + aPurgeRev + " should be true or false.\n" );
    } else {
      purgeRev = Boolean.parseBoolean( aPurgeRev );
    }
    
    if ( aContentTarget == null ) {
      aContentTarget = "C";
    }
    String upperContentTarget = aContentTarget.toUpperCase();
    if ( upperContentTarget.equals( "C" ) || upperContentTarget.equals( "CONTENT" ) ) {
      fileFilter = "*.kjb|*.ktr";
      repositoryPath = "/";
      purgeShared = false;
    } else if ( upperContentTarget.equals( "S" ) || upperContentTarget.equals( "SHARED" ) ) {
      fileFilter = "*";
      repositoryPath = " ";
      purgeShared = true;
    } else if ( upperContentTarget.equals( "B" ) || upperContentTarget.equals( "BOTH" ) ) {
      fileFilter = "*.kjb|*.ktr";
      repositoryPath = "/";
      purgeShared = true;
    } else {
      errors.append( OPTION_PREFIX + CONTENT_TARGET + "=" + aContentTarget
          + " is invalid.  Valid values are CONTENT, SHARED, BOTH, C, S, OR B and are not case sensitive.\n" );
    }

    if ( aDelFrom != null ) {
      SimpleDateFormat sdf = new SimpleDateFormat( deleteBeforeDateFormat );
      sdf.setLenient( false );
      try {
        sdf.parse( aDelFrom );
        delFrom = aDelFrom;
      } catch ( ParseException e ) {
        errors.append( OPTION_PREFIX + DEL_DATE + "=" + aDelFrom + " should be defined in " + deleteBeforeDateFormat
            + " format.\n" );
      }
    }

    if ( aVerCount != null ) {
      try {
        verCount = Integer.parseInt( aVerCount );
      } catch ( NumberFormatException e ) {
        errors.append( OPTION_PREFIX + VER_COUNT + "=" + aVerCount + " should be an integer.\n" );
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

    help.append( "\n\nOptions for purge-utility:" );

    help.append( optionHelp( URL, "Required, should be a valid parameter where the Data Integration"
        + " Server resides, (eg. " + URL + "=http://localhost:9080)" ) );
    help.append( optionHelp( USER, "Required User Name" ) );
    help.append( optionHelp( PASS, "Required password for user" ) );
    help.append( optionHelp( CONTENT_TARGET, "What to purge (CONTENT = Content Files, SHARED = Shared Objects"
        + ", BOTH = both content and shared files).  It is acceptable to use just the first character"
        + " of the value C, S, or B.  If omitted CONTENT will be targeted." ) );

    help.append( "\n" );
    help.append( indentFormat(
        "The command line must include at least one of these options, and that option must be set to a non-false value.  "
            + "These options describe what to do with the files defined by the " + CONTENT_TARGET + " option.", 0, 0 ) );
    help.append( optionHelp( VER_COUNT,
        "If present, delete all version history except the last 'versionCount' versions.  Should be an integer, (eg. "
            + VER_COUNT + "=2)" ) );
    help.append( optionHelp( DEL_DATE,
        "If present, delete all version history created prior to this date in MM/dd/YYYY format" + ", (eg. " + DEL_DATE
            + "=12/01/2014)." ) );
    help.append( optionHelp( PURGE_FILES, "If " + PURGE_FILES
        + "=true, then physically remove all the files specified by '" + CONTENT_TARGET
        + ".  Note that this option PERMINENTLY erases files." ) );
    help.append( optionHelp( PURGE_REV, " iF " + PURGE_REV + "=true, all version history will be removed, but the"
        + " current files will remain unchanged." ) );

    help.append( "\n\nOptional Parameters:" );
    help.append( optionHelp( LOG_LEVEL, "valid values are ALL,DEBUG,ERROR,FATAL,TRACE,INFO,OFF,TRACE,WARN."
        + "  Defaults to " + LOG_LEVEL + "=INFO if omitted." ) );

    help.append( "\n\nEXAMPLES:" );
    help.append( indentFormat( "1) purge-utility " + OPTION_PREFIX + URL + "=http://localhost:9080 " + OPTION_PREFIX
        + USER + "=admin " + OPTION_PREFIX + PASS + "=password " + OPTION_PREFIX + CONTENT_TARGET + "=CONTENT "
        + OPTION_PREFIX + PURGE_FILES + "=true", 0, 3 ) );
    help.append( indentFormat( "Purge ALL Transforms and Jobs in the repository.", 3, 3 ) );

    help.append( indentFormat( "2) purge-utility " + OPTION_PREFIX + URL + "=http://localhost:9080 " + OPTION_PREFIX
        + USER + "=admin " + OPTION_PREFIX + PASS + "=password " + OPTION_PREFIX + CONTENT_TARGET + "=BOTH "
        + OPTION_PREFIX + PURGE_REV + "=true", 0, 3 ) );
    help.append( indentFormat(
        "Purge ALL version history of ALL Transforms, Jobs, and shared objects in the repository.  The current version will remain.",
        3, 3 ) );

    help.append( indentFormat( "3) purge-utility " + OPTION_PREFIX + URL + "=http://localhost:9080 " + OPTION_PREFIX
        + USER + "=admin " + OPTION_PREFIX + PASS + "=password " + OPTION_PREFIX + DEL_DATE + "=11/30/2014", 0, 3 ) );
    help.append( indentFormat( "Purge any version history for content that is not the current version and was created"
        + " prior to midnight on November 30 2014.", 3, 3 ) );

    return help.toString();
  }
  
  private String optionHelp( String optionName, String descriptionText ) {
    int indentFirstLine = 2;
    int indentBalance = Math.min( OPTION_PREFIX.length() + optionName.length() + 4, 10 );
    return indentFormat( OPTION_PREFIX + optionName + ": " + descriptionText, indentFirstLine, indentBalance);
   }
  
  private String indentFormat( String unformattedText, int indentFirstLine, int indentBalance ) {
    final int maxWidth = 79;
    String leadLine = WordUtils.wrap( unformattedText, maxWidth - indentFirstLine );
    StringBuilder result = new StringBuilder();
    result.append( "\n" );
    if ( leadLine.indexOf( NEW_LINE ) == -1 ) {
      result.append( NEW_LINE ).append( StringUtils.repeat( " ", indentFirstLine ) ).append( unformattedText );
    } else {
      int lineBreakPoint = leadLine.indexOf( NEW_LINE );
      String indentString = StringUtils.repeat( " ", indentBalance );
      result.append( NEW_LINE ).append( StringUtils.repeat( " ", indentFirstLine ) ).append(
          leadLine.substring( 0, lineBreakPoint ) );
      String formattedText = WordUtils.wrap( unformattedText.substring( lineBreakPoint ), maxWidth - indentBalance );
      for ( String line : formattedText.split( NEW_LINE ) ) {
        result.append( NEW_LINE ).append( indentString ).append( line );
      }
    }
    return result.toString();
  }
  
  private String writeLog( String message ) throws Exception {
    String logName;
    if ( logFile != null ) {
      logName = logFile;
    } else {
      DateFormat df = new SimpleDateFormat( "YYYYMMdd-HHmmss" );
      logName = "purge-utility-log-" + df.format( new Date() ) + ".txt";
    }
    File file = new File( logName );
    FileOutputStream fout = FileUtils.openOutputStream( file );
    IOUtils.copy( IOUtils.toInputStream( message ), fout );
    fout.close();
    return logName;
  }
  
  private static void writeOut( String message, boolean isError ) {
    if ( isError ) {
      System.err.println( message );
      System.exit( 1 );
    } else {
      System.out.println( message );
    }
  }

  private static void writeOut( Throwable t ) {
    t.printStackTrace();
    System.exit( 1 );
  }
}
