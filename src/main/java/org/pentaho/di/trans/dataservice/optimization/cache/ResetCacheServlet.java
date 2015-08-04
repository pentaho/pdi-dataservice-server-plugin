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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Strings;
import com.google.common.net.MediaType;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * This servlet allows a user to clear the data service cache
 *
 * @author nhudak
 */
@CarteServlet(
  id = "ds_clearServiceCache",
  name = "PDI Data Service: ServiceCache reset",
  description = "Clear a data service Cache"
)
public class ResetCacheServlet extends BaseHttpServlet implements CartePluginInterface {
  private static final String NAME_PARAMETER = "name";
  private final ServiceCacheFactory factory;

  public ResetCacheServlet( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  private static final String CONTEXT_PATH = "/clearDataServiceCache";

  @Override public String toString() {
    return "Data Service Cache Reset";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override public void doGet( HttpServletRequest req, HttpServletResponse resp ) throws IOException {
    doPost( req, resp );
  }

  @Override protected void doPost( HttpServletRequest request, HttpServletResponse response ) throws IOException {
    String name = request.getParameter( NAME_PARAMETER );

    if ( Strings.isNullOrEmpty( name ) ) {
      response.sendError( HttpServletResponse.SC_BAD_REQUEST, NAME_PARAMETER + " not specified" );
      return;
    }

    response.setStatus( HttpServletResponse.SC_OK );
    response.setContentType( MediaType.PLAIN_TEXT_UTF_8.toString() );

    PrintWriter writer = response.getWriter();
    for ( Cache cache : factory.getCache( name ).asSet() ) {
      cache.clear();
      writer.println( "Cleared cache: " + cache.getName() );
    }
    writer.println( "Done" );
  }
}
