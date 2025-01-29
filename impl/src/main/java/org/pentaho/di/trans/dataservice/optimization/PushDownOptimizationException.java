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


package org.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.core.exception.KettleException;

/**
 * @author nhudak
 */
public class PushDownOptimizationException extends KettleException {
  public PushDownOptimizationException() {
    super();
  }

  public PushDownOptimizationException( String message ) {
    super( message );
  }

  public PushDownOptimizationException( String message, Throwable cause ) {
    super( message, cause );
  }
}
