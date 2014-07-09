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
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.web.http.api.resources.FileResource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;

/**
 * Created by pminutillo on 7/7/14.
 */
@Path("/pur-repository-plugin/api/revision")
public class RevisionResource {

  private static boolean versioningEnabled = false;

  static{
    boolean isVersioningEnabled = PentahoSystem.get(
        Boolean.class,
        "versioningEnabled",
        PentahoSessionHolder.getSession()
    );

    versioningEnabled = isVersioningEnabled;

  }

  IUnifiedRepository repository;
  Repository diRepository;

  IRevisionService revisionService = null;

  public RevisionResource(IUnifiedRepository unifiedRepository, Repository diRepository) {
    this.diRepository = diRepository;
    this.repository = unifiedRepository;
  }

  /**
   * Retrieves the version history of a selected repository file
   *
   * @param pathId (colon separated path for the repository file)
   * @return file properties object <code> RepositoryFileDto </code>
   */
  @GET
  @Path("{pathId : .}/revisions")
  @Produces({APPLICATION_XML, APPLICATION_JSON})
  public Response doGetVersions(@PathParam("pathId") String pathId) {

    Serializable fileId = null;
    List<ObjectRevision> revisions = null;

    RepositoryFile repositoryFile = repository.getFile(FileResource.idToPath(pathId));
    if (repositoryFile != null) {
      fileId = repositoryFile.getId();
    }
    if (fileId != null) {
      try {
        revisionService = (IRevisionService) diRepository.getService(IRevisionService.class);
        revisions = revisionService.getRevisions(new StringObjectId("fileId"));
      } catch (KettleException e) {
        return Response.serverError().build();
      }

      ArrayList<Object> responseList = new ArrayList<Object>();
      RevisionsResponseList versionsResponseList = new RevisionsResponseList();
      for (ObjectRevision dto : revisions) {
        responseList.add(dto);
      }
      versionsResponseList.setList(responseList);
      return Response.ok(versionsResponseList).build();
    } else {
      return Response.serverError().build();
    }
  }

  /**
   * Get version enabled flag
   *
   * @return
   */
  @GET
  @Path("/versioningEnabled")
  public Response getVersioningEnabled() {
    return Response.ok(
      Boolean.toString(versioningEnabled)
    ).build();
  }

  /**
   * Set version enabled flag
   *
   * @param versioningEnabled
   * @return
   */
/*  @POST
  @Path("/versioningEnabled")
  public Response postVersioningEnabled(String versioningEnabled) {
    IPentahoObjectFactory objectFactory = PentahoSystem.getObjectFactory();
    if (objectFactory instanceof IPentahoDefinableObjectFactory) {
      IPentahoDefinableObjectFactory definableObjectFactory = (IPentahoDefinableObjectFactory) objectFactory;
      definableObjectFactory.defineInstance("versioningEnabled", Boolean.parseBoolean(versioningEnabled));
    }

    return getVersioningEnabled();
  }*/
}
