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
