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


package org.pentaho.di.trans.dataservice.execution;

import com.google.common.base.Throwables;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;

/**
 * @author nhudak
 */
public class PrepareExecution implements Runnable {
  private final Trans trans;

  public PrepareExecution( Trans trans ) {
    this.trans = trans;
  }

  @Override public void run() {
    try {
      trans.prepareExecution( null );
    } catch ( KettleException e ) {
      Throwables.propagate( e );
    }
  }
}
