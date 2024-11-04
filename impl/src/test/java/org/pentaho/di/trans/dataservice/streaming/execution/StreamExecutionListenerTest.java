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


package org.pentaho.di.trans.dataservice.streaming.execution;

import io.reactivex.subjects.PublishSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.streaming.StreamList;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * {@link StreamExecutionListener} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class StreamExecutionListenerTest {
  private StreamExecutionListener streamExecutionListener;
  private StreamList<RowMetaAndData> streamList;
  private PublishSubject<List<RowMetaAndData>> consumer;
  List<RowMetaAndData> listConsumer;

  @Mock RowMetaAndData mockRowMetaAndData;
  @Mock RowMetaAndData mockRowMetaAndData2;

  @Before
  public void setup() throws Exception {
    streamList = new StreamList();

    listConsumer = new ArrayList<>( );
    consumer = PublishSubject.create();
    consumer.subscribe( rowMetaAndData -> {
      listConsumer.clear();
      listConsumer.addAll( rowMetaAndData );
    } );
  }

  @Test
  public void testGetCachedWindow() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.ROW_BASED, 10000, 1, 10000, 1000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertFalse( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachePreWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachePreWindow().get( 0 ) );
    streamList.add( mockRowMetaAndData2 );
    assertFalse( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 2, streamExecutionListener.getCachePreWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachePreWindow().get( 0 ) );
    assertSame( mockRowMetaAndData2, streamExecutionListener.getCachePreWindow().get( 1 ) );
  }

  @Test
  public void testGetCachedWindowTimeBased() throws Exception {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.TIME_BASED, 10000,10, 1000, 100000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    Thread.sleep( 100 ); //allow for row to be written
    assertFalse( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachePreWindow().size() );
    assertEquals( mockRowMetaAndData, streamExecutionListener.getCachePreWindow().get( 0 ) );
  }

  @Test
  public void testGetCachedWindowFallback() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.TIME_BASED, 10000, 10000, 1, 10000 );
    streamExecutionListener.unSubscribeStarter();

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
    streamList.add( mockRowMetaAndData2  );
    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, listConsumer.size() );
    assertSame( mockRowMetaAndData2, listConsumer.get( 0 ) );
  }

  @Test
  public void testGetCachedWindowRegular() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
    IDataServiceClientService.StreamingMode.ROW_BASED, 2, 1, 10000, 50000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertEquals( 1, listConsumer.size() ); //initial buffer - pre window
    streamList.add( mockRowMetaAndData2  );
    assertEquals( 2, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
    assertSame( mockRowMetaAndData2, listConsumer.get( 1 ) );
  }

  @Test
  public void testGetCachedWindowRegularZeroEvery() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.ROW_BASED, 2, 0, 10000, 50000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertEquals( 0, listConsumer.size() );
    streamList.add( mockRowMetaAndData2  );
    assertEquals( 2, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
    assertSame( mockRowMetaAndData2, listConsumer.get( 1 ) );
  }

  @Test
  public void testGetCachedWindowTimeBasedRegularZeroEvery() throws InterruptedException {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.TIME_BASED, 200, 0, 10000, 50000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertEquals( 0, listConsumer.size() );
    streamList.add( mockRowMetaAndData2  );
    assertEquals( 0, listConsumer.size() );
  }

  @Test
  public void testUnsubscribe() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.ROW_BASED, 10000, 1, 10000, 1000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertFalse( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachePreWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachePreWindow().get( 0 ) );
    streamExecutionListener.unSubscribe();
    streamList.add( mockRowMetaAndData2  );
    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
  }

  @Test
  public void testDoubleUnsubscribe() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(), rowMetaAndDataList -> consumer.onNext( rowMetaAndDataList ) ,
      IDataServiceClientService.StreamingMode.ROW_BASED, 10000, 1, 10000, 1000 );

    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    streamList.add( mockRowMetaAndData );
    assertFalse( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachePreWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachePreWindow().get( 0 ) );
    streamExecutionListener.unSubscribe();
    streamList.add( mockRowMetaAndData2  );
    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
    streamExecutionListener.unSubscribe();
    streamList.add( mockRowMetaAndData2  );
    assertTrue( streamExecutionListener.getCachePreWindow().isEmpty() );
    assertEquals( 1, listConsumer.size() );
    assertSame( mockRowMetaAndData, listConsumer.get( 0 ) );
  }
}
