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

import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.www.BaseCartePlugin;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryProvider;

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

  public RepositoryPublishServlet(  ) {
    PluginServiceLoader.registerService( this, KettleRepositoryProvider.class, new ServletRepositoryProvider( new ServletRepositoryAdapter( this ) ), 0 );
  }

  @Override public void handleRequest( CarteRequest request ) throws IOException {
    request.respond( 200 ).withMessage( "Repository Successfully Published" );
  }

  @Override public String getContextPath() {
    return CONTEXT_PATH;
  }
}
