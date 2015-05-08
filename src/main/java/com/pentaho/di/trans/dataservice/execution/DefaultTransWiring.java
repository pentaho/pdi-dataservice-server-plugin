/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.execution;

import com.google.common.base.Throwables;
import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

/**
 * @author nhudak
 */
public class DefaultTransWiring implements Runnable {
  private final DataServiceExecutor dataServiceExecutor;

  public DefaultTransWiring( DataServiceExecutor dataServiceExecutor ) {
    this.dataServiceExecutor = dataServiceExecutor;
  }

  @Override public void run() {
    // This is where we will inject the rows from the service transformation step
    //
    final RowProducer rowProducer;
    final Trans serviceTrans = dataServiceExecutor.getServiceTrans();

    try {
      rowProducer = dataServiceExecutor.addRowProducer();
    } catch ( KettleException e ) {
      throw Throwables.propagate( e );
    }

    // Now connect the 2 transformations with listeners and injector
    //
    StepInterface serviceStep = serviceTrans.findRunThread( dataServiceExecutor.getService().getStepname() );
    serviceStep.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        // Simply pass along the row to the other transformation (to the Injector step)
        //
        LogChannelInterface log = serviceTrans.getLogChannel();
        try {
          if ( log.isRowLevel() ) {
            log.logRowlevel( "Passing along row: " + rowMeta.getString( row ) );
          }
        } catch ( KettleValueException e ) {
          // Ignore errors
        }

        rowProducer.putRow( rowMeta, row );
      }
    } );

    // Let the other transformation know when there are no more rows
    //
    serviceTrans.addTransListener( new TransAdapter() {
      @Override
      public void transFinished( Trans trans ) throws KettleException {
        rowProducer.finished();
      }
    } );

    dataServiceExecutor.getGenTrans()
      .findRunThread( dataServiceExecutor.getResultStepName() )
      .addStepListener( new StepAdapter() {
        @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
          if ( serviceTrans.isRunning() ) {
            trans.getLogChannel().logBasic( "Query finished, stopping service transformation" );
            serviceTrans.stopAll();
          }
        }
      } );
  }

}
