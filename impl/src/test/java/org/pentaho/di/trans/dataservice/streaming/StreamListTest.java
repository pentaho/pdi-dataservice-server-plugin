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

package org.pentaho.di.trans.dataservice.streaming;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link StreamList} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamListTest {
  private StreamList<String> streamList;
  private String MOCK_MESSAGE = "Mock Message";
  private List<String> resultList;

  @Before
  public void setup() throws Exception {
    streamList = new StreamList();
  }

  @Test
  public void testAdd() {
    streamList.getStream().buffer( 1 ).subscribe( list -> resultList = list );
    streamList.add( MOCK_MESSAGE );
    assertEquals( 1, resultList.size() );
    assertEquals( MOCK_MESSAGE, resultList.get( 0 ) );
  }

  @Test
  public void testGetStream() {
    assertEquals( streamList.onAdd, streamList.getStream() );
  }
}
