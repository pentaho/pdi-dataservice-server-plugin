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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

/**
 * @author nhudak
 */
public class DataServiceValidationException extends KettleException {
  protected final DataServiceMeta dataServiceMeta;

  public DataServiceValidationException( DataServiceMeta dataServiceMeta, String message ) {
    super( message );
    this.dataServiceMeta = dataServiceMeta;
  }

  public DataServiceMeta getDataServiceMeta() {
    return dataServiceMeta;
  }
}
