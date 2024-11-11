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


package org.pentaho.di.trans.dataservice.ui;

import com.google.common.base.Strings;
import org.pentaho.ui.xul.binding.BindingConvertor;

import java.util.Map;
import java.util.Set;

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

  public static <K, V> BindingConvertor<Map<K, V>, Set<K>> keySet() {
    return new BindingConvertor<Map<K, V>, Set<K>>() {
      @Override public Set<K> sourceToTarget( Map<K, V> value ) {
        return value.keySet();
      }

      @Override public Map<K, V> targetToSource( Set<K> value ) {
        throw new AbstractMethodError( "Unable to convert a set to a map" );
      }
    };
  }

  public static BindingConvertor<Long, String> longToStringEmptyZero() {
    return new BindingConvertor<Long, String>() {
      @Override public String sourceToTarget( Long value ) {
        return value != null && value > 0L ? value.toString() : "";
      }

      @Override public Long targetToSource( String value ) {
        if ( value != null && !value.isEmpty() ) {
          try {
            return Long.valueOf( value );
          } catch ( NumberFormatException var3 ) {
            return new Long( 0L );
          }
        } else {
          return new Long( 0L );
        }
      }
    };
  }
}
