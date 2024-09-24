/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.www;

import com.google.common.collect.Lists;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;
import org.pentaho.di.www.BaseCartePlugin;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.Collection;
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
  description = "List all the available data services" )
public class ListDataServicesServlet extends BaseCartePlugin {

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/listServices";

  public static final String XML_TAG_SERVICES = "services";
  public static final String XML_TAG_SERVICE = "service";
  private final String CONTENT_CHARSET = "utf-8";
  private final DataServiceClient client;

  public ListDataServicesServlet( DataServiceClient client ) {
    this.client = client;
    this.log = client.getLogChannel();
  }

  @Override
  public void handleRequest( CarteRequest request ) throws IOException {
    final List<IThinServiceInformation> serviceInformation;
    try {
      Map<String, Collection<String>> requestParameters = request.getParameters();
      if ( requestParameters.isEmpty() || !requestParameters.containsKey( "serviceName" ) ) {
        serviceInformation = client.getServiceInformation();
      } else {
        serviceInformation = Lists.newArrayList();
        if ( requestParameters.containsKey( "serviceName" ) ) {
          Collection<String> servicesNames = requestParameters.get( "serviceName" );
          for ( String serviceName : servicesNames ) {
            ThinServiceInformation service = client.getServiceInformation( URLDecoder.decode( serviceName, CONTENT_CHARSET ) );
            if ( service != null ) {
              serviceInformation.add( service );
            }
          }
        }
      }
    } catch ( SQLException e ) {
      String msg = "Failed to retrieve service info";
      logError( msg, e );
      request.respond( 500 ).withMessage( msg );
      return;
    }

    request.respond( 200 )
      .with( "text/xml; charset=utf-8", new WriterResponse() {
        private void writeServiceXml( PrintWriter writer, IThinServiceInformation thinServiceInformation ) throws IOException {
          writer.println( XMLHandler.openTag( XML_TAG_SERVICE ) );

          writer.println( XMLHandler.addTagValue( "name", thinServiceInformation.getName() ) );
          writer.println( XMLHandler.addTagValue( "streaming", thinServiceInformation.isStreaming() ) );
          writer.println( thinServiceInformation.getServiceFields().getMetaXML() );

          writer.println( XMLHandler.closeTag( XML_TAG_SERVICE ) );
        }

        @Override
        public void write( PrintWriter writer ) throws IOException {
          writer.println( XMLHandler.getXMLHeader() );
          writer.println( XMLHandler.openTag( XML_TAG_SERVICES ) );

          for ( IThinServiceInformation thinServiceInformation : serviceInformation ) {

            String streamingParam = request.getParameter( "streaming" );

            if ( streamingParam == null ) {
              writeServiceXml( writer, thinServiceInformation );
            } else {
              boolean streaming = Boolean.parseBoolean( request.getParameter( "streaming" ) );

              if ( streaming == thinServiceInformation.isStreaming() ) {
                writeServiceXml( writer, thinServiceInformation );
              }
            }
          }
          writer.println( XMLHandler.closeTag( XML_TAG_SERVICES ) );
        }
      } );
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
