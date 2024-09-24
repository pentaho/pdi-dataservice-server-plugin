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

import com.google.common.base.Supplier;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.www.BaseHttpServlet;

/**
 * @author nhudak
 */
public class ServletRepositoryAdapter implements Supplier<Repository> {
  private final BaseHttpServlet servlet;

  public ServletRepositoryAdapter( BaseHttpServlet servlet ) {
    this.servlet = servlet;
  }

  @Override public Repository get() {
    try {
      return servlet.getTransformationMap().getSlaveServerConfig().getRepository();
    } catch ( KettleException e ) {
      servlet.logError( "Unable to connect to repository", e );
      return null;
    }
  }
}
