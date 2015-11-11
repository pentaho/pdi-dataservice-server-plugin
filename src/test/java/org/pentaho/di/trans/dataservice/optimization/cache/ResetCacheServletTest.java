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
