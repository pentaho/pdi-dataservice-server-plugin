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

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.net.MediaType;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This servlet allows a user to get data from a "service" which is a transformation step.
 *
 * @author matt
 */
@CarteServlet(
  id = "sql",
  name = "Get data from a data service",
  description = "Get data from a transformation data service using SQL"
  )
public class TransDataServlet extends BaseHttpServlet implements CartePluginInterface {
  private static Class<?> PKG = TransDataServlet.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private static final long serialVersionUID = 3634806745372015720L;

  private static final String MAX_ROWS = "MaxRows";
  private static final String SQL = "SQL";
  private final DataServiceClient client;

  public static final String CONTEXT_PATH = "/sql";

  public TransDataServlet( DataServiceContext context ) {
    this.client = context.createClient( new ServletRepositoryAdapter( this ) );
  }

  public void doPut( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    doGet( request, response );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetStatusServlet.StatusRequested" ) );
    }

    String
        sqlQuery =
        !Strings.isNullOrEmpty( request.getParameter( SQL ) ) ? request.getParameter( SQL ) : request.getHeader( SQL );
    if ( Strings.isNullOrEmpty( sqlQuery ) ) {
      response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      response.setContentType( MediaType.PLAIN_TEXT_UTF_8.toString() );
      response.getWriter().println( "SQL query not specified" );
      return;
    }
    String
        maxRowsValue =
        !Strings.isNullOrEmpty( request.getParameter( MAX_ROWS ) ) ? request.getParameter( MAX_ROWS )
            : request.getHeader( MAX_ROWS );
    final int maxRows = Const.toInt( maxRowsValue, -1 );

    final String debugTransFile = request.getParameter( "debugtrans" );

    // Parse the variables in the request header...
    //
    Map<String, String> parameters = getParametersFromRequestHeader( request );

    try {
      Query query = client.prepareQuery( sqlQuery, maxRows, parameters );

      // For logging and tracking purposes, let's expose both the service transformation as well
      // as the generated transformation on this very carte instance
      //
      List<Trans> transList = query.getTransList();
      for ( Trans trans : transList ) {
        monitorTransformation( trans );
      }

      // Log the generated transformation if needed
      //
      if ( !Strings.isNullOrEmpty( debugTransFile ) && !transList.isEmpty() ) {
        saveGeneratedTransformation( Iterables.getLast( transList ).getTransMeta(), debugTransFile );
      }

      response.setStatus( HttpServletResponse.SC_OK );
      response.setContentType( "binary/jdbc" );
      query.writeTo( response.getOutputStream() );

    } catch ( Exception e ) {
      log.logError( "Error executing SQL query: " + sqlQuery, e );
      response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      response.getWriter().println( Strings.nullToEmpty( e.getMessage() ).trim() );
    }
  }

  private void monitorTransformation( Trans trans ) {
    TransMeta transMeta = trans.getTransMeta();
    TransExecutionConfiguration executionConfiguration = new TransExecutionConfiguration();
    TransConfiguration config = new TransConfiguration( transMeta, executionConfiguration );
    transformationMap.addTransformation( transMeta.getName(), trans.getContainerObjectId(), trans, config );
  }

  private void saveGeneratedTransformation( TransMeta genTransMeta, String debugTrans ) {
    try {
      FileOutputStream fos = new FileOutputStream( debugTrans );
      fos.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
      fos.write( genTransMeta.getXML().getBytes( Const.XML_ENCODING ) );
      fos.close();
    } catch ( Exception e ) {
      logError( "Unable to write dynamic transformation to file", e );
    }
  }

  public static Map<String, String> getParametersFromRequestHeader( HttpServletRequest request ) {
    Map<String, String> parameters = new HashMap<String, String>();
    Enumeration<?> parameterNames = request.getParameterNames();
    while ( parameterNames.hasMoreElements() ) {
      String fullName = (String) parameterNames.nextElement();
      if ( fullName.startsWith( "PARAMETER_" ) ) {
        String parameterName = fullName.substring( "PARAMETER_".length() );
        String value = request.getParameter( fullName );
        if ( !Const.isEmpty( parameterName ) ) {
          parameters.put( parameterName, Const.NVL( value, "" ) );
        }
      }
    }

    return parameters;
  }

  public String toString() {
    return "Transformation data service";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }

}
