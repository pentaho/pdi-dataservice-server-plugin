/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
