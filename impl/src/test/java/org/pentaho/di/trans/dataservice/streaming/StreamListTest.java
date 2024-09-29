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


package org.pentaho.di.trans.dataservice.streaming;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link StreamList} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
