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

package org.pentaho.di.trans.dataservice.clients;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.BaseTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

/**
 * {@link QueryServiceDelegate} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class QueryServiceDelegateTest extends BaseTest {
  private static final String TEST_SQL_QUERY = "SELECT * FROM " + DATA_SERVICE_NAME;
  private static final int MAX_ROWS = 100;
  private static final Map<String, String> parameters = new HashMap<>();

  private QueryServiceDelegate client;
  private List<Query.Service> queryServices;

  @Mock QueryServiceDelegate service1;
  @Mock QueryServiceDelegate service2;
  @Mock Query query1;
  @Mock Query query2;

  @Before
  public void setUp() throws Exception {
    queryServices = new ArrayList<>( );
    queryServices.add( service1 );
    queryServices.add( service2 );

    client = new QueryServiceDelegate( queryServices );
  }

  @Test
  public void testPrepareQuery() throws KettleException {
    when( service1.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, 0, 0, 0,
      parameters ) ).thenReturn( query1 );
    when( service2.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, 0, 0, 0,
      parameters ) ).thenReturn( query2 );

    Query result = client.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, parameters );
    assertEquals( query1, result );

    when( service1.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, 0, 0, 0,
      parameters ) ).thenReturn( null );

    result = client.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, parameters );
    assertEquals( query2, result );

    when( service2.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, 0, 0, 0,
      parameters ) ).thenReturn( null );

    result = client.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, parameters );
    assertNull( result );
  }
}
