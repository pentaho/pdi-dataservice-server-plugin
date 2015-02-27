/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.di.trans.dataservice.www;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.di.www.JobMap;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.api.IMetaStore;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;

/**
 * This servlet allows a user to get data from a "service" which is a transformation step.
 *
 * @author matt
 */
@CarteServlet(
  id = "listServices",
  name = "List data services",
  description = "List all the available data services"
)
public class ListDataServicesServlet extends BaseHttpServlet implements CartePluginInterface {
  private static Class<?> PKG = ListDataServicesServlet.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/listServices";

  public static final String XML_TAG_SERVICES = "services";
  public static final String XML_TAG_SERVICE = "service";

  public ListDataServicesServlet() {
  }

  public ListDataServicesServlet( TransformationMap transformationMap, JobMap jobMap ) {
    super( transformationMap, jobMap );
  }

  public void doPut( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    doGet( request, response );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "LisDataServicesServlet.ListRequested" ) );
    }
    response.setStatus( HttpServletResponse.SC_OK );

    Map<String, String> parameters = TransDataServlet.getParametersFromRequestHeader( request );


    response.setContentType( "text/xml" );

    response.getWriter().println( XMLHandler.getXMLHeader() );
    response.getWriter().println( XMLHandler.openTag( XML_TAG_SERVICES ) );

    // Copy the list locally so we can add the current repository services...
    //
    IMetaStore metaStore = transformationMap.getSlaveServerConfig().getMetaStore();

    // Add possible services from the repository...
    //
    Repository repository = null;
    List<DataServiceMeta> dataServices = Collections.emptyList();
    try {
      repository = transformationMap.getSlaveServerConfig().getRepository(); // loaded lazily
      dataServices = DataServiceMetaStoreUtil.getDataServices( metaStore );
    } catch ( Exception e ) {
      log.logError( "Unable to list extra repository services", e );
    }

    for ( DataServiceMeta service : dataServices ) {
      response.getWriter().println( XMLHandler.openTag( XML_TAG_SERVICE ) );
      response.getWriter().println( XMLHandler.addTagValue( "name", service.getName() ) );

      // Also include the row layout of the service step.
      //
      try {
        TransMeta transMeta = null;
        if ( repository != null && service.getTransObjectId() != null ) {
          StringObjectId objectId = new StringObjectId( service.getTransObjectId() );
          transMeta = repository.loadTransformation( objectId, null );
        } else if ( repository != null && service.getName() != null ) {
          String path = "/";
          String name = service.getName();
          int lastSlashIndex = service.getName().lastIndexOf( '/' );
          if ( lastSlashIndex >= 0 ) {
            path = service.getName().substring( 0, lastSlashIndex + 1 );
            name = service.getName().substring( lastSlashIndex + 1 );
          }
          RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
          RepositoryDirectoryInterface rd = tree.findDirectory( path );
          if ( rd == null ) {
            rd = tree; // root
          }
          transMeta = repository.loadTransformation( name, rd, null, true, null );
        } else {
          transMeta = new TransMeta( service.getTransFilename() );
        }

        for ( String name : parameters.keySet() ) {
          transMeta.setParameterValue( name, parameters.get( name ) );
        }
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        response.getWriter().println( serviceFields.getMetaXML() );

      } catch ( Exception e ) {
        // Don't include details
        log.logError( "Unable to get fields for service " + service.getName() + ", transformation: " + service.getTransFilename() );
      }

      response.getWriter().println( XMLHandler.closeTag( XML_TAG_SERVICE ) );
    }
    response.getWriter().println( XMLHandler.closeTag( XML_TAG_SERVICES ) );
  }

  public String toString() {
    return "List data services";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
