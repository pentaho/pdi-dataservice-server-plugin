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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
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

  public static final String CONTEXT_PATH = "/sql";
  private final DataServiceClient client;

  public TransDataServlet( DataServiceContext context ) {
    client = context.getDataServiceClient();
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
    response.setStatus( HttpServletResponse.SC_OK );

    response.setContentType( "binary/jdbc" );
    response.setBufferSize( 10000 );
    // response.setHeader("Content-Length", Integer.toString(Integer.MAX_VALUE));

    String sqlQuery = request.getHeader( "SQL" );
    final int maxRows = Const.toInt( request.getHeader( "MaxRows" ), -1 );

    final String debugTransFile = request.getParameter( "debugtrans" );

    // Parse the variables in the request header...
    //
    Map<String, String> parameters = getParametersFromRequestHeader( request );

    try {
      SQL sql = new SQL( sqlQuery );
      if ( sql.getServiceName() == null || sql.getServiceName().equals( DataServiceClient.DUMMY_TABLE_NAME ) ) {
        // Support for SELECT 1 and SELECT 1 FROM dual
        client.writeDummyRow( sql, new DataOutputStream( response.getOutputStream() ) );
      } else {
        // Update client with configured repository and metastore
        client.setRepository( transformationMap.getSlaveServerConfig().getRepository() );
        client.setMetaStore( transformationMap.getSlaveServerConfig().getMetaStore() );

        // Pass query to client
        DataServiceExecutor executor = client.buildExecutor( sql ).
            parameters( parameters ).
            rowLimit( maxRows ).
            build();

        executor.executeQuery( new DataOutputStream( response.getOutputStream() ) );

        // For logging and tracking purposes, let's expose both the service transformation as well
        // as the generated transformation on this very carte instance
        //
        TransMeta serviceTransMeta = executor.getServiceTransMeta();
        Trans serviceTrans = executor.getServiceTrans();
        if ( serviceTrans != null ) {
          // not dual
          TransConfiguration serviceTransConfiguration = new TransConfiguration( serviceTransMeta, new TransExecutionConfiguration() );
          transformationMap.addTransformation( serviceTransMeta.getName(), serviceTrans.getContainerObjectId(), serviceTrans, serviceTransConfiguration );
        }

        // And the generated transformation...
        //
        TransMeta genTransMeta = executor.getGenTransMeta();
        Trans genTrans = executor.getGenTrans();
        TransConfiguration genTransConfiguration = new TransConfiguration( genTransMeta, new TransExecutionConfiguration() );
        transformationMap.addTransformation( genTransMeta.getName(), genTrans.getContainerObjectId(), genTrans, genTransConfiguration );

        // Log the generated transformation if needed
        //
        if ( !Const.isEmpty( debugTransFile ) ) {
          // Store it to temp file for debugging!
          //
          try {
            FileOutputStream fos = client.getDebugFileOutputStream( debugTransFile );
            fos.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
            fos.write( genTransMeta.getXML().getBytes( Const.XML_ENCODING ) );
            fos.close();
          } catch ( Exception e ) {
            logError( "Unable to write dynamic transformation to file", e );
          }
        }

        executor.waitUntilFinished();
      }

    } catch ( Exception e ) {
      log.logError( "Error executing SQL query: " + sqlQuery, e );
      response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      response.getWriter().println( e.getMessage().trim() );
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
    this.log =  log;
  }
}
