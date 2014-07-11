/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.repository.pur;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.repository.pur.ActiveCache.ExecutorServiceGetter;

public class ActiveCacheTest {
  private final ExecutorServiceGetter synchronousExecutorServiceGetter = new ExecutorServiceGetter() {

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Override
    public ExecutorService getExecutor() {
      ExecutorService mockExecutor = mock( ExecutorService.class );
      when( mockExecutor.submit( any( Callable.class ) ) ).thenAnswer( new Answer<Future>() {

        @Override
        public Future answer( final InvocationOnMock invocation ) throws Throwable {
          Future result = mock( Future.class );
          Object callableResult = ( (Callable) invocation.getArguments()[0] ).call();
          when( result.get() ).thenReturn( callableResult );
          return result;
        }
      } );
      return mockExecutor;
    }
  };

  @Test
  public void testActiveCacheLoadsWhenNull() throws Exception {
    long timeout = 100;
    @SuppressWarnings( "unchecked" )
    ActiveCacheLoader<String, String> mockLoader = mock( ActiveCacheLoader.class );
    ActiveCache<String, String> cache = new ActiveCache<String, String>( mockLoader, timeout );
    String testKey = "TEST-KEY";
    String testResult = "TEST-RESULT";
    when( mockLoader.load( testKey ) ).thenReturn( testResult );
    assertEquals( testResult, cache.get( testKey ) );
    verify( mockLoader, times( 1 ) ).load( testKey );
  }

  @Test
  public void testActiveCacheLoadsWhenTimedOut() throws Exception {
    long timeout = 100;
    @SuppressWarnings( "unchecked" )
    ActiveCacheLoader<String, String> mockLoader = mock( ActiveCacheLoader.class );
    ActiveCache<String, String> cache = new ActiveCache<String, String>( mockLoader, timeout );
    String testKey = "TEST-KEY";
    String testResult = "TEST-RESULT";
    String testResult2 = "TEST-RESULT-2";
    when( mockLoader.load( testKey ) ).thenReturn( testResult ).thenReturn( testResult2 );
    assertEquals( testResult, cache.get( testKey ) );
    Thread.sleep( timeout + 10 );
    assertEquals( testResult2, cache.get( testKey ) );
    verify( mockLoader, times( 2 ) ).load( testKey );
  }

  @Test
  public void testActiveCachePreemtivelyReloadsWhenHalfwayToTimeout() throws Exception {
    long timeout = 100;
    @SuppressWarnings( "unchecked" )
    ActiveCacheLoader<String, String> mockLoader = mock( ActiveCacheLoader.class );
    ActiveCache<String, String> cache =
        new ActiveCache<String, String>( mockLoader, timeout, synchronousExecutorServiceGetter );
    String testKey = "TEST-KEY";
    String testResult = "TEST-RESULT";
    String testResult2 = "TEST-RESULT-2";
    when( mockLoader.load( testKey ) ).thenReturn( testResult ).thenReturn( testResult2 );
    assertEquals( testResult, cache.get( testKey ) );
    Thread.sleep( 55 );
    // Trigger reload, we should get original result back here as it hasn't timed out
    assertEquals( testResult, cache.get( testKey ) );
    // Should get new value when it's ready (immediately due to synchronous executor)
    assertEquals( testResult2, cache.get( testKey ) );
    verify( mockLoader, times( 2 ) ).load( testKey );
  }

  @Test
  public void testActiveCacheDoesntCacheExceptions() throws Exception {
    long timeout = 100;
    @SuppressWarnings( "unchecked" )
    ActiveCacheLoader<String, String> mockLoader = mock( ActiveCacheLoader.class );
    ActiveCache<String, String> cache = new ActiveCache<String, String>( mockLoader, timeout );
    String testKey = "TEST-KEY";
    Exception testResult = new Exception( "TEST-RESULT" );
    String testResult2 = "TEST-RESULT-2";
    when( mockLoader.load( testKey ) ).thenThrow( testResult ).thenReturn( testResult2 );
    try {
      cache.get( testKey );
      fail();
    } catch ( Exception e ) {
      assertEquals( testResult, e );
    }
    assertEquals( testResult2, cache.get( testKey ) );
    verify( mockLoader, times( 2 ) ).load( testKey );
  }
}
