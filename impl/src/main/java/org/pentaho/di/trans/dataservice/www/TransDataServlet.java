/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.www;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.www.BaseCartePlugin;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
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
  description = "Get data from a transformation data service using SQL" )
public class TransDataServlet extends BaseCartePlugin {
  private static final long serialVersionUID = 3634806745372015720L;

  private static final String MAX_ROWS = "MaxRows";
  private static final String WINDOW_MODE = "WindowMode";
  private static final String WINDOW_SIZE = "WindowSize";
  private static final String WINDOW_EVERY = "WindowEvery";
  private static final String WINDOW_LIMIT = "WindowLimit";
  private static final String SQL = "SQL";
  private final DataServiceClient client;

  public static final String CONTEXT_PATH = "/sql";

  public TransDataServlet( DataServiceClient client ) {
    this.client = client;
    this.log = client.getLogChannel();
  }

  public void handleRequest( CarteRequest request ) throws IOException {
    String
      sqlQuery =
      !Strings.isNullOrEmpty( request.getParameter( SQL ) ) ? request.getParameter( SQL ) : request.getHeader( SQL );
    if ( Strings.isNullOrEmpty( sqlQuery ) ) {
      String sqlParamMissing = "SQL not specified";
      logError( sqlParamMissing );
      request.respond( 400 ).withMessage( sqlParamMissing );
      return;
    }

    try {
      String serviceName = new SQL( sqlQuery ).getServiceName();

      DataServiceMeta clientMeta =  !Strings.isNullOrEmpty( serviceName )
        ? client.getServiceMeta( serviceName ) : null;

      boolean isStreaming = clientMeta != null && clientMeta.isStreaming();

      String
        maxRowsValue =
        !Strings.isNullOrEmpty( request.getParameter( MAX_ROWS ) ) ? request.getParameter( MAX_ROWS )
          : request.getHeader( MAX_ROWS );

      final int maxRows = Const.toInt( maxRowsValue, -1 );

      final String debugTransFile = request.getParameter( "debugtrans" );

      Map<String, String> parameters = collectParameters( request.getParameters() );

      final Query query;

      if ( isStreaming ) {
        String
          windowModeValue =
          !Strings.isNullOrEmpty( request.getParameter( WINDOW_MODE ) ) ? request.getParameter( WINDOW_MODE )
            : request.getHeader( WINDOW_MODE );

        String
          windowSizeValue =
          !Strings.isNullOrEmpty( request.getParameter( WINDOW_SIZE ) ) ? request.getParameter( WINDOW_SIZE )
            : request.getHeader( WINDOW_SIZE );

        String
          windowEveryValue =
          !Strings.isNullOrEmpty( request.getParameter( WINDOW_EVERY ) ) ? request.getParameter( WINDOW_EVERY )
            : request.getHeader( WINDOW_EVERY );

        String
          windowLimitValue =
          !Strings.isNullOrEmpty( request.getParameter( WINDOW_LIMIT ) ) ? request.getParameter( WINDOW_LIMIT )
            : request.getHeader( WINDOW_LIMIT );

        final IDataServiceClientService.StreamingMode windowMode
          = IDataServiceClientService.StreamingMode.TIME_BASED.toString().equalsIgnoreCase( windowModeValue )
          ? IDataServiceClientService.StreamingMode.TIME_BASED : IDataServiceClientService.StreamingMode.ROW_BASED;

        final boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );

        final long windowSize = Const.toLong( windowSizeValue,
          rowBased ? clientMeta.getRowLimit() : clientMeta.getTimeLimit() );
        final long windowEvery = Const.toLong( windowEveryValue,
          rowBased ? clientMeta.getRowLimit() : clientMeta.getTimeLimit() );
        final long windowLimit = Const.toLong( windowLimitValue,
          rowBased ? clientMeta.getTimeLimit() : clientMeta.getRowLimit() );

        query = client.prepareQuery( sqlQuery, windowMode, windowSize, windowEvery, windowLimit,
          parameters );
      } else {
        query = client.prepareQuery( sqlQuery, maxRows, parameters );
      }

      // For logging and tracking purposes, let's expose both the service transformation as well
      // as the generated transformation on this very carte instance
      List<Trans> transList = query.getTransList();
      for ( Trans trans : transList ) {
        monitorTransformation( trans );
      }

      if ( !Strings.isNullOrEmpty( debugTransFile ) && !transList.isEmpty() ) {
        saveGeneratedTransformation( Iterables.getLast( transList ).getTransMeta(), debugTransFile );
      }

      request.respond( 200 )
        .with( "binary/jdbc", new OutputStreamResponse() {
          @Override public void write( OutputStream outputStream ) throws IOException {
            query.writeTo( outputStream );
          }
        } );


    } catch ( Exception e ) {
      logError( "Error executing SQL query: " + sqlQuery, e );
      request
        .respond( 400 )
        .withMessage( Strings.nullToEmpty( e.getMessage() ).trim() );
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

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  private Map<String, String> collectParameters( Map<String, Collection<String>> map ) {
    Map<String, String> parameters = Maps.newHashMap();
    for ( Map.Entry<String, Collection<String>> parameterEntry : map.entrySet() ) {
      String name = parameterEntry.getKey();
      Iterator<String> value = parameterEntry.getValue().iterator();
      if ( name.startsWith( IDataServiceClientService.PARAMETER_PREFIX ) && value.hasNext() ) {
        String firstVal = value.next();
        parameters.put( name.substring( IDataServiceClientService.PARAMETER_PREFIX.length() ), firstVal );
        if ( value.hasNext() ) {
          logDetailed(
            String.format(
              "More than one value associated with param %s.  Setting to first found (%s)",
              name, firstVal ) );
        }
      }
    }
    return ImmutableMap.copyOf( parameters );
  }


}
