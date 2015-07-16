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

import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
  private final DataServiceClient client;

  public ListDataServicesServlet( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
    client = new DataServiceClient( context );
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
    response.setContentType( "text/xml" );

    response.getWriter().println( XMLHandler.getXMLHeader() );
    response.getWriter().println( XMLHandler.openTag( XML_TAG_SERVICES ) );

    List<ThinServiceInformation> serviceInformation = Collections.emptyList();
    try {
      client.setRepository( transformationMap.getSlaveServerConfig().getRepository() );
      client.setMetaStore( transformationMap.getSlaveServerConfig().getMetaStore() );
      serviceInformation = client.getServiceInformation();
    } catch ( Exception e ) {
      log.logError( "Unable to list extra repository services", e );
    }

    for ( ThinServiceInformation thinServiceInformation : serviceInformation ) {
      String serviceName = thinServiceInformation.getName();
      response.getWriter().println( XMLHandler.openTag( XML_TAG_SERVICE ) );
      response.getWriter().println( XMLHandler.addTagValue( "name", serviceName ) );

      // Also include the row layout of the service step.
      //
      RowMetaInterface serviceFields = thinServiceInformation.getServiceFields();
      response.getWriter().println( serviceFields.getMetaXML() );

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
