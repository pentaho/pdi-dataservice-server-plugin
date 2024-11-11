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


package org.pentaho.di.trans.dataservice.execution;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowAdapter;

import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/10/15.
 */
class DefaultTransWiringRowAdapter extends RowAdapter {
  private final Trans serviceTrans;
  private final Trans genTrans;
  private final RowProducer rowProducer;

  public DefaultTransWiringRowAdapter( Trans serviceTrans, Trans genTrans, RowProducer rowProducer ) {
    this.serviceTrans = serviceTrans;
    this.genTrans = genTrans;
    this.rowProducer = rowProducer;
  }

  @Override
  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    // Simply pass along the row to the other transformation (to the Injector step)
    //
    LogChannelInterface log = serviceTrans.getLogChannel();
    try {
      if ( log.isRowLevel() ) {
        log.logRowlevel( DataServiceConstants.PASSING_ALONG_ROW + rowMeta.getString( row ) );
      }
    } catch ( KettleValueException e ) {
      // Ignore errors
    }

    try {
      Object[] rowData = rowMeta.cloneRow( row );
      while ( !rowProducer.putRowWait( rowMeta, rowData, 1, TimeUnit.SECONDS ) && genTrans.isRunning() ) {
        // Row queue was full, try again
        if ( log.isRowLevel() ) {
          log.logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
        }
      }
    } catch ( KettleValueException e ) {
      throw new KettleStepException( e );
    }
  }
}
