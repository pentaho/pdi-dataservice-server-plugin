/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2014 Pentaho Corporation..  All rights reserved.
 */
package com.pentaho.di.purge;

import static javax.ws.rs.core.MediaType.WILDCARD;

import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

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
  IPurgeService purgeService;

  public PurgeResource( IUnifiedRepository unifiedRepository ) {
    this.repository = unifiedRepository;
  }

  // **** NEED TO MAKE THIS THREADSAFE ****
  private void tryRepository() {
    if ( purgeService == null ) {
      this.purgeService = new UnifiedRepositoryPurgeService( this.repository );
    }
  }

  @GET
  @Path( "{pathId : .+}/purge" )
  @Consumes( { WILDCARD } )
  public Response doDeleteRevisions( @PathParam( "pathId" ) String pathId,
      @DefaultValue( "false" ) @QueryParam( "purgeFiles" ) boolean purgeFiles,
      @DefaultValue( "false" ) @QueryParam( "purgeRevisions" ) boolean purgeRevisions,
      @DefaultValue( "false" ) @QueryParam( "purgeSharedObjects" ) boolean purgeSharedObjects,
      @DefaultValue( "-1" ) @QueryParam( "versionCount" ) int versionCount,
      @QueryParam( "deleteBeforeDate" ) Date deleteBeforeDate,
      @DefaultValue( "*" ) @QueryParam( "fileFilter" ) String fileFilter ) {

    // A version count of 0 is illegal.
    if ( versionCount == 0 ) {
      return Response.serverError().build();
    }

    if ( purgeRevisions && ( versionCount > 0 || deleteBeforeDate != null ) ) {
      purgeRevisions = false;
    }
    
    tryRepository();
    PurgeUtilitySpecification purgeSpecification = new PurgeUtilitySpecification();
    purgeSpecification.setPath( idToPath( pathId ) );
    purgeSpecification.setPurgeFiles( purgeFiles );
    purgeSpecification.setPurgeRevisions( purgeRevisions );
    purgeSpecification.setSharedObjects( purgeSharedObjects );
    purgeSpecification.setVersionCount( versionCount );
    purgeSpecification.setBeforeDate( deleteBeforeDate );
    purgeSpecification.setFileFilter( fileFilter );

    try {
      purgeService.doDeleteRevisions( purgeSpecification );
    } catch ( PurgeDeletionException e ) {
      return Response.serverError().build();
    }

    return Response.ok().build();
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
}
