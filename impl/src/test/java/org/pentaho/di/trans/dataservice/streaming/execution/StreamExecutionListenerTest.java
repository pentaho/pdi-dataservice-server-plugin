/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.streaming.execution;

import io.reactivex.subjects.PublishSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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
@RunWith( MockitoJUnitRunner.class )
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
