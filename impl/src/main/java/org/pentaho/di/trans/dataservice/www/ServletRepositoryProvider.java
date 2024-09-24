/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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
