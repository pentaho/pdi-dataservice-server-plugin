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

import org.osgi.framework.BundleContext;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.www.BaseCartePlugin;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryProvider;

import java.io.IOException;

/**
 * Created by bmorrise on 9/9/16.
 */
@CarteServlet(
  id = "repoPublish",
  name = "Repository Publish",
  description = "Publish the accessible servlet repository" )
public class RepositoryPublishServlet extends BaseCartePlugin {

  public static final String CONTEXT_PATH = "/repositoryPublished";

  public RepositoryPublishServlet( BundleContext bundleContext ) {
    ServletRepositoryProvider repositoryProvider =
      new ServletRepositoryProvider( new ServletRepositoryAdapter( this ) );
    bundleContext.registerService( KettleRepositoryProvider.class.getName(), repositoryProvider, null );
  }

  @Override public void handleRequest( CarteRequest request ) throws IOException {
    request.respond( 200 ).withMessage( "Repository Successfully Published" );
  }

  @Override public String getContextPath() {
    return CONTEXT_PATH;
  }


}
