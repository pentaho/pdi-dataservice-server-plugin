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
import org.pentaho.di.trans.step.StepInterface;

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
  }

}
