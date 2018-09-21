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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

/**
 * {@link WindowParametersHelper} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class WindowParametersHelperTest {

  @Test
  public void testGetCacheKey() {
    assertNull( WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 0, 1, 10000, 10000, 1000, 99999 ) );
    assertEquals( "99999-queryROW_BASED-10-1-10000-1000", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 10, 1, 10000, 10000, 1000, 99999 ) );
    assertEquals( "99999-queryROW_BASED-1-1-1-2", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 10, 5, 1, 2, 1000, 99999 ) );
    assertEquals( "88888-queryROW_BASED-1-1-1-2", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 10, 5, 1, 2, 1000, 88888 ) );
    assertEquals( "99999-queryTIME_BASED-10-1-1000-10000", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.TIME_BASED, 10, 1, 10000, 10000, 1000, 99999 ) );
    assertEquals( "99999-queryTIME_BASED-2-2-1-2", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.TIME_BASED, 10, 5, 1, 2, 1000, 99999 ) );
    assertEquals( "77777-queryTIME_BASED-2-2-1-2", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.TIME_BASED, 10, 5, 1, 2, 1000, 77777 ) );

    assertEquals( "99999-queryROW_BASED-10-0-10000-1000", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 10, -1, 10000, 10000, 1000, 99999 ) );
    assertEquals( "55555-queryROW_BASED-10-0-10000-1000", WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, 10, -1, 10000, 10000, 1000, 55555 ) );

    assertNull( WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, -10, 1, 10000, 10000, 1000, 99999 ) );

    assertNull( WindowParametersHelper.getCacheKey( "query", IDataServiceClientService.StreamingMode.ROW_BASED, -10, -1, 10000, 10000, 1000, 99999 ) );
  }

  @Test
  public void testGetMaxTimeRowMode() {
    assertEquals( 1000, WindowParametersHelper.getMaxTime( 1000, 0, true ) );
    assertEquals( 10, WindowParametersHelper.getMaxTime( 1000, 10, true ) );
    assertEquals( 1000, WindowParametersHelper.getMaxTime( 1000, 10000, true ) );
  }

  @Test
  public void testGetMaxTimeTimeMode() {
    assertEquals( 1000, WindowParametersHelper.getMaxTime( 1000, 0, false ) );
    assertEquals( 1000, WindowParametersHelper.getMaxTime( 1000, 10, false ) );
    assertEquals( 1000, WindowParametersHelper.getMaxTime( 1000, 10000, false ) );
  }

  @Test
  public void testGetMaxRowsRowMode() {
    assertEquals( 1000, WindowParametersHelper.getMaxRows( 1000, 0, false ) );
    assertEquals( 1000, WindowParametersHelper.getMaxRows( 1000, 10, false ) );
    assertEquals( 1000, WindowParametersHelper.getMaxRows( 1000, 10000, false ) );
  }

  @Test
  public void testGetMaxRowsTimeMode() {
    assertEquals( 1000, WindowParametersHelper.getMaxRows( 1000, 0, true ) );
    assertEquals( 10, WindowParametersHelper.getMaxRows( 1000, 10, true ) );
    assertEquals( 1000, WindowParametersHelper.getMaxRows( 1000, 10000, true ) );
  }

  @Test
  public void testGetWindowEveryRowMode() {
    assertEquals( 100, WindowParametersHelper.getWindowEvery( 1000, false, 100, 10000 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowEvery( 1000, false, 2000, 500 ) );
    assertEquals( 100, WindowParametersHelper.getWindowEvery( 1000, false, 100, 500 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowEvery( 1000, false, 2000, 3000 ) );
  }

  @Test
  public void testGetWindowEveryTimeMode() {
    assertEquals( 1000, WindowParametersHelper.getWindowEvery( 1000, true, 100, 10000 ) );
    assertEquals( 500, WindowParametersHelper.getWindowEvery( 1000, true, 2000, 500 ) );
    assertEquals( 500, WindowParametersHelper.getWindowEvery( 1000, true, 100, 500 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowEvery( 1000, true, 2000, 3000 ) );
  }

  @Test
  public void testGetWindowSizeRowMode() {
    assertEquals( 100, WindowParametersHelper.getWindowSize( 1000, false, 100, 10000 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowSize( 1000, false, 2000, 500 ) );
    assertEquals( 100, WindowParametersHelper.getWindowSize( 1000, false, 100, 500 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowSize( 1000, false, 2000, 3000 ) );
  }

  @Test
  public void testGetWindowSizeTimeMode() {
    assertEquals( 1000, WindowParametersHelper.getWindowSize( 1000, true, 100, 10000 ) );
    assertEquals( 500, WindowParametersHelper.getWindowSize( 1000, true, 2000, 500 ) );
    assertEquals( 500, WindowParametersHelper.getWindowSize( 1000, true, 100, 500 ) );
    assertEquals( 1000, WindowParametersHelper.getWindowSize( 1000, true, 2000, 3000 ) );
  }

}
