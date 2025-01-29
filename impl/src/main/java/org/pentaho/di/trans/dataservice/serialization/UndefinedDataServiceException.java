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


package org.pentaho.di.trans.dataservice.serialization;

import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.text.MessageFormat;

/**
 * @author nhudak
 */
public class UndefinedDataServiceException extends DataServiceValidationException {

  public UndefinedDataServiceException( DataServiceMeta dataServiceMeta ) {
    super( dataServiceMeta, MessageFormat.format( "Data Service {0} is undefined.", dataServiceMeta.getName() ) );
  }

  public UndefinedDataServiceException( DataServiceMeta dataServiceMeta, String message ) {
    super( dataServiceMeta, message );
  }
}
