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

import org.pentaho.di.repository.Repository;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryProvider;

/**
 * Created by bmorrise on 9/9/16.
 */
public class ServletRepositoryProvider implements KettleRepositoryProvider {

  private final ServletRepositoryAdapter repositoryAdapter;

  public ServletRepositoryProvider( ServletRepositoryAdapter repositoryAdapter ) {
    this.repositoryAdapter = repositoryAdapter;
  }

  @Override public Repository getRepository() {
    return repositoryAdapter.get();
  }
}
