/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.www;

import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
  private final DataServiceMetaStoreUtil metaStoreUtil;

  public TransDataServlet( DataServiceMetaStoreUtil metaStoreUtil ) {
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
      logDebug( BaseMessages.getString( PKG, "GetStatusServlet.StatusRequested" ) );
    }
    response.setStatus( HttpServletResponse.SC_OK );

    response.setContentType( "binary/jdbc" );
    response.setBufferSize( 10000 );
    // response.setHeader("Content-Length", Integer.toString(Integer.MAX_VALUE));

    final OutputStream outputStream = response.getOutputStream();
    final DataOutputStream dos = new DataOutputStream( outputStream );

    String sqlQuery = request.getHeader( "SQL" );
    final int maxRows = Const.toInt( request.getHeader( "MaxRows" ), -1 );

    final String debugTransFile = request.getParameter( "debugtrans" );

    // Parse the variables in the request header...
    //
    Map<String, String> parameters = getParametersFromRequestHeader( request );

    final AtomicLong dataSize = new AtomicLong( 0L );
    try {

      // Add possible services from the repository...
      //
      Repository repository = transformationMap.getSlaveServerConfig().getRepository();
      DelegatingMetaStore metaStore = transformationMap.getSlaveServerConfig().getMetaStore();
      List<DataServiceMeta> dataServices = metaStoreUtil.getMetaStoreFactory( metaStore ).getElements();

      // Execute the SQL using a few transformations...
      //
      final DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sqlQuery ), dataServices ).
        parameters( parameters ).
        rowLimit( maxRows ).
        lookupServiceTrans( repository ).
        build();

      // First write the service name and the metadata
      //
      dos.writeUTF( executor.getServiceName() );

      // Then send the transformation names and carte container IDs
      //
      dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), true ) );
      String serviceContainerObjectId = UUID.randomUUID().toString();
      dos.writeUTF( serviceContainerObjectId );
      dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), false ) );
      String genContainerObjectId = UUID.randomUUID().toString();
      dos.writeUTF( genContainerObjectId );

      final AtomicBoolean firstRow = new AtomicBoolean( true );

      // Now execute the query transformation(s) and pass the data to the output stream...
      //
      // TODO: allow global repository configuration in the services config file
      //
      final AtomicBoolean wroteRowMeta = new AtomicBoolean( false );

      executor.executeQuery( new RowAdapter() {
        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

          // On the first row, write the metadata...
          //
          try {
            if ( firstRow.compareAndSet( true, false ) ) {
              rowMeta.writeMeta( dos );
              wroteRowMeta.set( true );
            }
            rowMeta.writeData( dos, row );
            dataSize.set( dos.size() );
          } catch ( Exception e ) {
            if ( !executor.getServiceTrans().isStopped() ) {
              throw new KettleStepException( e );
            }
          }
        }
      } );

      // For logging and tracking purposes, let's expose both the service transformation as well
      // as the generated transformation on this very carte instance
      //
      TransMeta serviceTransMeta = executor.getServiceTransMeta();
      Trans serviceTrans = executor.getServiceTrans();
      if ( serviceTrans != null ) {
        // not dual
        TransConfiguration serviceTransConfiguration = new TransConfiguration( serviceTransMeta, new TransExecutionConfiguration() );
        transformationMap.addTransformation( serviceTransMeta.getName(), serviceContainerObjectId, serviceTrans, serviceTransConfiguration );
      }

      // And the generated transformation...
      //
      TransMeta genTransMeta = executor.getGenTransMeta();
      Trans genTrans = executor.getGenTrans();
      TransConfiguration genTransConfiguration = new TransConfiguration( genTransMeta, new TransExecutionConfiguration() );
      transformationMap.addTransformation( genTransMeta.getName(), genContainerObjectId, genTrans, genTransConfiguration );

      // Log the generated transformation if needed
      //
      if ( !Const.isEmpty( debugTransFile ) ) {
        // Store it to temp file for debugging!
        //
        try {
          FileOutputStream fos = new FileOutputStream( debugTransFile );
          fos.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
          fos.write( genTransMeta.getXML().getBytes( Const.XML_ENCODING ) );
          fos.close();
        } catch ( Exception fnfe ) {
          throw new KettleException( fnfe );
        }
      }

      executor.waitUntilFinished();

      // Check if no row metadata was written.  The client is still going to expect it...
      // Since we know it, we'll pass it.
      //
      if ( !wroteRowMeta.get() ) {
        RowMetaInterface stepFields = executor.getGenTransMeta().getStepFields( executor.getResultStepName() );
        stepFields.writeMeta( dos );
      }

      // The client has to come back to the GetTransStatus servlet to check for errors
      // So that's all we do here...
      //

    } catch ( Exception e ) {
      log.logError( "Error executing SQL query: " + sqlQuery, e );
      response.sendError( 500, Const.getStackTracker( e ) );
    } finally {
      System.out.println( "bytes written: " + dataSize );
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

}
