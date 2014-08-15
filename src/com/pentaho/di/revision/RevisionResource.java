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
package com.pentaho.di.revision;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.pur.PurObjectRevision;
import org.pentaho.di.repository.pur.UnifiedRepositoryRevisionService;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.repository2.unified.jcr.JcrRepositoryFileUtils;
import org.pentaho.platform.web.http.api.resources.utils.FileUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;

/**
 * Created by pminutillo on 7/7/14.
 *
 * Provide REST endpoints for revision API. These methods will
 * provide the current status of the versioning and version
 * comments enabled flags
 */
@Path("/pur-repository-plugin/api/revision")
public class RevisionResource {

  IUnifiedRepository repository;

  IRevisionService revisionService = null;

  /**
   *
   * @param unifiedRepository
   * @param diRepository
   */
  public RevisionResource(IUnifiedRepository unifiedRepository) {
    this.repository = unifiedRepository;
    // Is there a better way to get the revisionService
    this.revisionService = new UnifiedRepositoryRevisionService(unifiedRepository, null);
  }

  /**
   * Retrieves the version history of a selected repository file
   *
   * @param pathId (colon separated path for the repository file)
   * <pre function="syntax.xml">
   *    :path:to:file:id
   * </pre>
   * @return file revisions objects <code> purObjectRevisions </code>
   * <pre function="syntax.xml">
   * &lt;purObjectRevisions&gt;
   * &lt;revision&gt;
   * &lt;versionId&gt;1.0&lt;/versionId&gt;
   * &lt;creationDate&gt;2014-07-22T14:42:46.029-04:00&lt;/creationDate&gt;
   * &lt;login&gt;admin&lt;/login&gt;
   * &lt;comment&gt;JMeter test&lt;/comment&gt;
   * &lt;/revision&gt;
   * &lt;/purObjectRevisions&gt;
   * </pre>
   */
  @GET
  @Path("{pathId : .+}/revisions")
  @Produces({APPLICATION_XML, APPLICATION_JSON})
  public Response doGetVersions(@PathParam("pathId") String pathId) {

    Serializable fileId = null;
    List<ObjectRevision> originalRevisions = null;

    RepositoryFile repositoryFile = repository.getFile( FileUtils.idToPath( pathId ));
    if (repositoryFile != null) {
      fileId = repositoryFile.getId();
    }
    if (fileId != null) {
      try {
        originalRevisions = revisionService.getRevisions(new StringObjectId(fileId.toString()));
      } catch (KettleException e) {
        return Response.serverError().build();
      }

      List<PurObjectRevision> revisions = new ArrayList();
      for(ObjectRevision revision : originalRevisions ){
        revisions.add((PurObjectRevision) revision);
      }

      GenericEntity<List<PurObjectRevision>> genericRevisionsEntity = new GenericEntity<List<PurObjectRevision>>(revisions){};

      return Response.ok(genericRevisionsEntity).build();
    } else {
      return Response.serverError().build();
    }
  }

  /**
   * Get version enabled flag
   *
   * @return string representing boolean versioning enabled flag value
   * <pre function="syntax.xml">
   *   true
   * </pre>
   */
  @GET
  @Path("/versioningEnabled")
  public Response getVersioningEnabled() {
    return Response.ok( Boolean.toString( JcrRepositoryFileUtils.getVersioningEnabled() ) ).build();
  }

  /**
   * Get version comments enabled flag
   *
   * @return string representing boolean version comments enabled flag value
   * <pre function="syntax.xml">
   *   true
   * </pre>
   */
  @GET
  @Path("/versionCommentsEnabled")
  public Response getVersionCommentsEnabled() {
    return Response.ok( Boolean.toString( JcrRepositoryFileUtils.getVersionCommentsEnabled() ) ).build();
  }
}
