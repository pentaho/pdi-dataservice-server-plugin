/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;

/**
 * {@link DataServiceConstants} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceConstantsTest {
  @Test( expected = InvocationTargetException.class )
  public void testPrivateConstructor() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException {
    Constructor<DataServiceConstants> constructor = DataServiceConstants.class.getDeclaredConstructor();
    assertTrue( Modifier.isPrivate( constructor.getModifiers() ) );
    constructor.setAccessible( true );
    constructor.newInstance();
  }
}
