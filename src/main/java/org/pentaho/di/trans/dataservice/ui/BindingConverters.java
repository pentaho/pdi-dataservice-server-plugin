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
