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

package org.pentaho.di.trans.dataservice.testing.answers;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;

/**
 * @author nhudak
 */
public class ReturnsSelf implements Answer<Object> {
  public static final ReturnsSelf RETURNS_SELF = new ReturnsSelf();
  private final Answer<Object> defaultAnswer;

  public ReturnsSelf() {
    this( Mockito.RETURNS_DEFAULTS );
  }

  public ReturnsSelf( Answer<Object> defaultAnswer ) {
    this.defaultAnswer = defaultAnswer;
  }

  @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
    Method method = invocation.getMethod();
    if ( method.getReturnType().isInstance( invocation.getMock() ) ) {
      return invocation.getMock();
    } else {
      return defaultAnswer.answer( invocation );
    }
  }
}
