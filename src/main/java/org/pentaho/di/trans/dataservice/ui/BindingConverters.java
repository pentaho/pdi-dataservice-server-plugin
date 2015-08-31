/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui;

import com.google.common.base.Strings;
import org.pentaho.ui.xul.binding.BindingConvertor;

/**
 * @author nhudak
 */
public class BindingConverters {

  private BindingConverters() {
  }

  public static BindingConvertor<String, Boolean> stringIsEmpty() {
    return new BindingConvertor<String, Boolean>() {
      @Override public Boolean sourceToTarget( String value ) {
        return Strings.isNullOrEmpty( value );
      }

      @Override public String targetToSource( Boolean value ) {
        throw new AbstractMethodError( "Boolean to String conversion is not supported" );
      }
    };
  }

  public static BindingConvertor<Boolean, Boolean> not() {
    return new BindingConvertor<Boolean, Boolean>() {
      @Override public Boolean sourceToTarget( Boolean value ) {
        return !value;
      }

      @Override public Boolean targetToSource( Boolean value ) {
        return !value;
      }
    };
  }
}
