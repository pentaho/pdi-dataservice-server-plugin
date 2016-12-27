/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.execution;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/10/15.
 */
public class DefaultTransWiringRowAdapterTest {
  private Trans serviceTrans;
  private Trans genTrans;
  private RowProducer rowProducer;
  private DefaultTransWiringRowAdapter defaultTransWiringRowAdapter;
  private LogChannelInterface logChannelInterface;
  private RowMetaInterface rowMetaInterface;
  private Object[] row;
  private String testString;
  private Object[] clonedRow;

  @Before
  public void setup() throws KettleValueException {
    serviceTrans = mock( Trans.class );
    genTrans = mock( Trans.class );
    rowProducer = mock( RowProducer.class );
    logChannelInterface = mock( LogChannelInterface.class );
    when( serviceTrans.getLogChannel() ).thenReturn( logChannelInterface );
    defaultTransWiringRowAdapter = new DefaultTransWiringRowAdapter( serviceTrans, genTrans, rowProducer, 10 );

    rowMetaInterface = mock( RowMetaInterface.class );
    this.testString = "testString";
    row = new Object[] { testString };
    clonedRow = new Object[] { testString };
    when( rowMetaInterface.cloneRow( row ) ).thenReturn( clonedRow );
    when( genTrans.isRunning() ).thenReturn( true );
  }

  @Test
  public void testRowWrittenEventSuccessLogging() throws KettleStepException {
    when( logChannelInterface.isRowLevel() ).thenReturn( true );
    when( rowProducer.putRowWait( rowMetaInterface, row, 1, TimeUnit.SECONDS ) ).thenReturn( false, true );
    defaultTransWiringRowAdapter.rowWrittenEvent( rowMetaInterface, row );
    verify( rowProducer, times( 2 ) ).putRowWait( same( rowMetaInterface ), eq( row ), eq( 1L ), eq( TimeUnit.SECONDS ) );
    verify( logChannelInterface ).logRowlevel( startsWith( DefaultTransWiringRowAdapter.PASSING_ALONG_ROW ) );
    verify( logChannelInterface ).logRowlevel( DefaultTransWiringRowAdapter.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
  }

  @Test
  public void testRowWrittenEventSuccessErrorLogging() throws KettleStepException, KettleValueException {
    when( logChannelInterface.isRowLevel() ).thenReturn( true );
    when( rowMetaInterface.getString( row ) ).thenThrow( new KettleValueException() );
    when( rowProducer.putRowWait( rowMetaInterface, row, 1, TimeUnit.SECONDS ) ).thenReturn( false, true );
    defaultTransWiringRowAdapter.rowWrittenEvent( rowMetaInterface, row );
    verify( rowProducer, times( 2 ) ).putRowWait( same( rowMetaInterface ), eq( row ), eq( 1L ), eq( TimeUnit.SECONDS ) );
    verify( logChannelInterface ).logRowlevel( DefaultTransWiringRowAdapter.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
  }

  @Test
  public void testRowWrittenEventSuccessNoLogging() throws KettleStepException {
    when( rowProducer.putRowWait( rowMetaInterface, row, 1, TimeUnit.SECONDS ) ).thenReturn( false, true );
    defaultTransWiringRowAdapter.rowWrittenEvent( rowMetaInterface, row );
    verify( rowProducer, times( 2 ) ).putRowWait( same( rowMetaInterface ), eq( row ), eq( 1L ), eq( TimeUnit.SECONDS ) );
    verify( logChannelInterface, never() ).logRowlevel( anyString() );
  }

  @Test( expected = KettleStepException.class )
  public void testRowWrittenEventException() throws KettleValueException, KettleStepException {
    rowMetaInterface = mock( RowMetaInterface.class );
    KettleValueException kettleValueException = new KettleValueException();
    when( rowMetaInterface.cloneRow( any( Object[].class ) ) ).thenThrow( kettleValueException );
    when( rowProducer.putRowWait( rowMetaInterface, row, 1, TimeUnit.SECONDS ) ).thenReturn( false, true );
    try {
      defaultTransWiringRowAdapter.rowWrittenEvent( rowMetaInterface, row );
    } catch ( KettleStepException e ) {
      assertEquals( kettleValueException, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testRowWrittenAbortsWhenGenTransIsNotRunning() throws KettleStepException {
    when( genTrans.isRunning() ).thenReturn( false );
    for( int i = 0; i < 11; i++) {
      defaultTransWiringRowAdapter.rowWrittenEvent( rowMetaInterface, row );
    }
    verify( serviceTrans ).stopAll();
    verifyNoMoreInteractions( rowProducer );
  }
}
