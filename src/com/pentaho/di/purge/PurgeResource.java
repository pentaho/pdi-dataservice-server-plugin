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

import static javax.ws.rs.core.MediaType.WILDCARD;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.pentaho.di.ui.repository.pur.services.IPurgeService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.util.RepositoryPathEncoder;

/**
 * Created by tkafalas 7/14/14.
 */
@Path( "/pur-repository-plugin/api/purge" )
public class PurgeResource {

  public static final String PATH_SEPARATOR = "/"; //$NON-NLS-1$

  IUnifiedRepository repository;

  public PurgeResource( IUnifiedRepository unifiedRepository ) {
    this.repository = unifiedRepository;
  }

  @GET
  @Path( "{pathId : .+}/purge" )
  @Consumes( { WILDCARD } )
  @Produces( MediaType.TEXT_HTML )
  public Response doDeleteRevisions( @PathParam( "pathId" ) String pathId,
      @DefaultValue( "false" ) @QueryParam( "purgeFiles" ) boolean purgeFiles,
      @DefaultValue( "false" ) @QueryParam( "purgeRevisions" ) boolean purgeRevisions,
      @DefaultValue( "false" ) @QueryParam( "purgeSharedObjects" ) boolean purgeSharedObjects,
      @DefaultValue( "-1" ) @QueryParam( "versionCount" ) int versionCount,
      @QueryParam( "deleteBeforeDate" ) Date deleteBeforeDate,
      @DefaultValue( "*" ) @QueryParam( "fileFilter" ) String fileFilter,
      @DefaultValue( "INFO" ) @QueryParam( "logLevel") String logLevelName ) {

    // A version count of 0 is illegal.
    if ( versionCount == 0 ) {
      return Response.serverError().build();
    }

    if ( purgeRevisions && ( versionCount > 0 || deleteBeforeDate != null ) ) {
      purgeRevisions = false;
    }
    
    IPurgeService purgeService = new UnifiedRepositoryPurgeService( this.repository );
    Level logLevel = Level.toLevel( logLevelName );
    
    PurgeUtilitySpecification purgeSpecification = new PurgeUtilitySpecification();
    purgeSpecification.setPath( idToPath( pathId ) );
    purgeSpecification.setPurgeFiles( purgeFiles );
    purgeSpecification.setPurgeRevisions( purgeRevisions );
    purgeSpecification.setSharedObjects( purgeSharedObjects );
    purgeSpecification.setVersionCount( versionCount );
    purgeSpecification.setBeforeDate( deleteBeforeDate );
    purgeSpecification.setFileFilter( fileFilter );
    purgeSpecification.setLogLevel( logLevel );

    //Initialize the logger
    ByteArrayOutputStream purgeUtilityStream = new ByteArrayOutputStream();
    PurgeUtilityLogger.createNewInstance( purgeUtilityStream, purgeSpecification.getPath(), logLevel );
    
    try {
      purgeService.doDeleteRevisions( purgeSpecification );
    } catch ( Exception e ) {
      PurgeUtilityLogger.getPurgeUtilityLogger().error( e );
      return Response.ok( encodeOutput( purgeUtilityStream), MediaType.TEXT_HTML ).build();
    }
    
    return Response.ok( encodeOutput( purgeUtilityStream), MediaType.TEXT_HTML ).build();
  }

  public static String idToPath( String pathId ) {
    String path = null;
    path = pathId.replaceAll( PATH_SEPARATOR, "" ); //$NON-NLS-1$
    path = RepositoryPathEncoder.decodeRepositoryPath( path );
    if ( !path.startsWith( PATH_SEPARATOR ) ) {
      path = PATH_SEPARATOR + path;
    }
    return path;
  }
  
  private String encodeOutput( ByteArrayOutputStream purgeUtilityStream ) {
    String responseBody = null;
    try {
      responseBody = purgeUtilityStream.toString( "UTF-8" );
    } catch ( UnsupportedEncodingException e ) {
      responseBody = purgeUtilityStream.toString();
    }
    return responseBody;
  }
}
