/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.www.BaseCartePlugin;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;

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
  private final DataServiceClient client;

  public ListDataServicesServlet( DataServiceClient client ) {
    this.client = client;
    this.log = client.getLogChannel();
  }

  @Override
  public void handleRequest( CarteRequest request ) throws IOException {
    final List<ThinServiceInformation> serviceInformation;
    try {
      serviceInformation = client.getServiceInformation();
    } catch ( SQLException e ) {
      String msg = "Failed to retrieve service info";
      logError( msg, e );
      request.respond( 500 ).withMessage( msg );
      return;
    }

    request.respond( 200 )
      .with( "text/xml", new WriterResponse() {
        @Override
        public void write( PrintWriter writer ) throws IOException {
          writer.println( XMLHandler.getXMLHeader() );
          writer.println( XMLHandler.openTag( XML_TAG_SERVICES ) );

          for ( ThinServiceInformation thinServiceInformation : serviceInformation ) {
            writer.println( XMLHandler.openTag( XML_TAG_SERVICE ) );

            writer.println( XMLHandler.addTagValue( "name", thinServiceInformation.getName() ) );
            writer.println( thinServiceInformation.getServiceFields().getMetaXML() );

            writer.println( XMLHandler.closeTag( XML_TAG_SERVICE ) );
          }
          writer.println( XMLHandler.closeTag( XML_TAG_SERVICES ) );
        }
      } );
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
