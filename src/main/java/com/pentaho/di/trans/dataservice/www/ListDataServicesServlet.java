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
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.di.www.JobMap;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.api.IMetaStore;

import com.pentaho.di.trans.dataservice.DataServiceMeta;

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
      dataServices = DataServiceMeta.getMetaStoreFactory( metaStore ).getElements();
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
