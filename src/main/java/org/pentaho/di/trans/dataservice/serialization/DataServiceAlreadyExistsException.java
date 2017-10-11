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

package org.pentaho.di.trans.dataservice.serialization;

import org.pentaho.di.trans.dataservice.DataServiceMeta;

import static org.pentaho.di.i18n.BaseMessages.getString;

/**
 * @author nhudak
 */
public class DataServiceAlreadyExistsException extends DataServiceValidationException {

  private static final Class<DataServiceAlreadyExistsException> PKG = DataServiceAlreadyExistsException.class;

  public DataServiceAlreadyExistsException( DataServiceMeta dataServiceMeta ) {
    this( dataServiceMeta, getString( PKG, "Messages.SaveError.NameConflict", dataServiceMeta.getName() ) );
  }

  public DataServiceAlreadyExistsException( DataServiceMeta dataServiceMeta, String message ) {
    super( dataServiceMeta, message );
  }
}
