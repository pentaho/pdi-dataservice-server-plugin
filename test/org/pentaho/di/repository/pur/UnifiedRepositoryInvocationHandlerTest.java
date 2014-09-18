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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.ConnectException;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.repository.KettleRepositoryLostException;

public class UnifiedRepositoryInvocationHandlerTest {
  private static interface IFace {
    Object doNotThrowException();
    Object throwSomeException();
    Object throwChainedConnectException();
  };
  
  private static final Object returnValue = "return-value";
  private static final RuntimeException rte = new RuntimeException("some-exception"); 
  private static final ConnectException connectException = new ConnectException();
  
  private static final IFace wrappee = new IFace(){

    @Override
    public Object doNotThrowException() {
      return returnValue;
    }

    @Override
    public Object throwSomeException() {
      throw rte;
    }

    @Override
    public Object throwChainedConnectException() {
      throw new RuntimeException("wrapper-exception", connectException);
    }
    
  };
  
  IFace testee;
  
  @Before
  public void setUp() {
    testee = UnifiedRepositoryInvocationHandler.forObject( wrappee, IFace.class );
  }
  
  @Test
  public void testNormalCall() {
    assertEquals( "the method did not return what was expected", returnValue, testee.doNotThrowException() );
  }
  
  @Test
  public void testThrowingSomeException() {
    try {
      testee.throwSomeException();
    } catch ( RuntimeException actual ) {
      assertEquals( "did not get the expected runtime exception", rte, actual );
    }
  }
  
  @Test
  public void testThrowingConnectException() {
    try {
      testee.throwChainedConnectException();
    } catch ( KettleRepositoryLostException krle ) {
      Throwable found = krle;
      while(found != null) {
        if(connectException.equals( found )) {
          break;
        }
        found = found.getCause();
      }
      assertNotNull( "Should have found the original ConnectException" );
    } catch ( Throwable other ) {
      fail("Should not catch something other than KettleRepositoryLostException");
    }
  }
}
