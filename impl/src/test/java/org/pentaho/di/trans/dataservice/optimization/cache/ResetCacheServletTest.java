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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.www.CarteRequestHandler;

import javax.cache.Cache;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
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

    lenient().when( request.getMethod() ).thenReturn( "POST" );

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
