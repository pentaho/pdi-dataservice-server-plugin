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
