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

import io.reactivex.Observable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.streaming.StreamList;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * {@link StreamExecutionListener} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamExecutionListenerTest {
  private StreamExecutionListener streamExecutionListener;
  private StreamList<RowMetaAndData> streamList;
  private Observable<List<RowMetaAndData>> buffer;
  private Observable<List<RowMetaAndData>> fallbackBuffer;

  @Mock RowMetaAndData mockRowMetaAndData;
  @Mock RowMetaAndData mockRowMetaAndData2;

  @Before
  public void setup() throws Exception {
    streamList = new StreamList();
    buffer = streamList.getStream().buffer( 1 );
    fallbackBuffer = streamList.getStream().buffer( 1000 );

    streamExecutionListener = new StreamExecutionListener( streamList.getStream(),
      IDataServiceClientService.StreamingMode.ROW_BASED, 1, 1, 10000, 1000 );
  }

  @Test
  public void testGetCachedWindow() {
    assertTrue( streamExecutionListener.getCachedWindow().isEmpty() );
    streamList.add( mockRowMetaAndData  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    streamList.add( mockRowMetaAndData2  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData2, streamExecutionListener.getCachedWindow().get( 0 ) );
  }

  @Test
  public void testStarterData() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(),
      IDataServiceClientService.StreamingMode.ROW_BASED, 1000, 1000, 10000, 10000 );

    assertTrue( streamExecutionListener.getCachedWindow().isEmpty() );
    streamList.add( mockRowMetaAndData  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    streamList.add( mockRowMetaAndData2  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 2, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    assertSame( mockRowMetaAndData2, streamExecutionListener.getCachedWindow().get( 1 ) );
  }

  @Test
  public void testGetCachedWindowFallback() {
    streamExecutionListener = new StreamExecutionListener( streamList.getStream(),
      IDataServiceClientService.StreamingMode.TIME_BASED, 10000, 10000, 1, 10000 );
    streamExecutionListener.unSubscribeStarter();

    assertTrue( streamExecutionListener.getCachedWindow().isEmpty() );
    streamList.add( mockRowMetaAndData  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    streamList.add( mockRowMetaAndData2  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData2, streamExecutionListener.getCachedWindow().get( 0 ) );
  }

  @Test
  public void testUnSubscribe() {
    assertTrue( streamExecutionListener.getCachedWindow().isEmpty() );
    streamList.add( mockRowMetaAndData  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    streamExecutionListener.unSubscribe();
    streamList.add( mockRowMetaAndData2  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
    streamExecutionListener.unSubscribe();
    streamList.add( mockRowMetaAndData2  );
    assertFalse( streamExecutionListener.getCachedWindow().isEmpty() );
    assertEquals( 1, streamExecutionListener.getCachedWindow().size() );
    assertSame( mockRowMetaAndData, streamExecutionListener.getCachedWindow().get( 0 ) );
  }
}
