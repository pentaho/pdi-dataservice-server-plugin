/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.www;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.metastore.api.IMetaStore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
  private final DataServiceMetaStoreUtil metaStoreUtil;

  public ListDataServicesServlet( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
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
      dataServices = metaStoreUtil.getMetaStoreFactory( metaStore ).getElements();
    } catch ( Exception e ) {
      log.logError( "Unable to list extra repository services", e );
    }

    for ( DataServiceMeta service : dataServices ) {
      response.getWriter().println( XMLHandler.openTag( XML_TAG_SERVICE ) );
      response.getWriter().println( XMLHandler.addTagValue( "name", service.getName() ) );

      // Also include the row layout of the service step.
      //
      try {
        TransMeta transMeta = service.lookupTransMeta( repository );

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
