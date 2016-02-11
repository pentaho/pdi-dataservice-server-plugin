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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.www.CarteRequestHandler;

import javax.cache.Cache;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class ResetCacheServletTest {
  @Mock ServiceCacheFactory factory;
  @Mock Cache<CachedService.CacheKey, CachedService> cache;

  @InjectMocks ResetCacheServlet servlet;

  @Mock CarteRequestHandler.CarteRequest request;
  @Mock CarteRequestHandler.CarteResponse response;
  private HashMultimap<String, String> parameterMap;

  @Before
  public void setUp() throws Exception {
    when( request.respond( anyInt() ) ).thenReturn( response );
    parameterMap = HashMultimap.create();
    when( request.getParameters() ).thenReturn( parameterMap.asMap() );

    when( request.getMethod() ).thenReturn( "POST" );

    when( factory.getCache( anyString() ) )
      .thenReturn( Optional.<Cache<CachedService.CacheKey, CachedService>>absent() );
    when( factory.getCache( "cache1" ) ).thenReturn( Optional.of( cache ) );
  }

  @Test
  public void testClearCache() throws Exception {
    parameterMap.putAll( "name", ImmutableList.of( "cache1", "noneSuch", "cache1" ) );

    servlet.handleRequest( request );

    verify( cache ).clear();
    verify( request ).respond( 200 );
  }

  @Test
  public void testBadRequest() throws Exception {
    servlet.handleRequest( request );
    verify( request ).respond( 400 );
  }
}
