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

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * Class to support retrieval of typed values and ValueMeta.
 * Used by param gen classes to ensure correct resolution of type info for fields
 * contained in SQLConditions, where type info is inferred at parse time but may be
 * inconsistent with ValueMeta from associated step fields retrieved during optimization.
 */
public class ValueMetaResolver {

  private final Map<String, ValueMetaInterface> fieldNameValueMetaMap;
  private static final String ANSI_DATE_LITERAL = "yyyy-MM-dd";
  private static final String ANSI_TIMESTAMP_LITERAL = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final String NUMERIC_LITERAL = "###0.###";
  public ValueMetaResolver( RowMetaInterface rowMeta ) {
    fieldNameValueMetaMap = FluentIterable.from( rowMeta.getValueMetaList() )
      .transform( setDefaultConversionMask )
      .uniqueIndex( new Function<ValueMetaInterface, String>() {
        @Override public String apply( ValueMetaInterface valueMetaInterface ) {
          return valueMetaInterface.getName();
        }
      } );
  }

  private static final Function<ValueMetaInterface, ValueMetaInterface> setDefaultConversionMask =
    new Function<ValueMetaInterface, ValueMetaInterface>() {
      @Override public ValueMetaInterface apply( ValueMetaInterface valueMetaInterface ) {
        valueMetaInterface = valueMetaInterface.clone();

        if ( Strings.isNullOrEmpty( valueMetaInterface.getConversionMask() ) ) {
          switch ( valueMetaInterface.getType() ) {
            case ValueMetaInterface.TYPE_DATE:
              valueMetaInterface.setConversionMask( ANSI_DATE_LITERAL );
              break;
            case ValueMetaInterface.TYPE_TIMESTAMP:
              valueMetaInterface.setConversionMask( ANSI_TIMESTAMP_LITERAL );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
            case ValueMetaInterface.TYPE_NUMBER:
            case ValueMetaInterface.TYPE_BIGNUMBER:
              valueMetaInterface.setConversionMask( NUMERIC_LITERAL );
          }
        }
        return valueMetaInterface;
      }
    };

  public ValueMetaInterface getValueMeta( String fieldName ) throws PushDownOptimizationException {
    ValueMetaInterface valueMeta = fieldNameValueMetaMap.get( fieldName );
    if ( valueMeta == null ) {
      throw new PushDownOptimizationException( String.format( "Field '%s' not found", fieldName ) );
    }
    return valueMeta;
  }

  /**
   * Return an object of a type consistent with the valueMeta associated with fieldName.
   */
  public Object getTypedValue( String fieldName, int originalType, Object value ) throws PushDownOptimizationException {
    ValueMetaInterface valueMeta = getValueMeta( fieldName );
    if ( valueMeta.getType() == originalType ) {
      // no need to convert
      return value;
    }
    return convertToType( valueMeta, originalType, value );
  }

  /**
   * Converts a semi-colon delimited list to an array of the type corresponding to fieldName.
   * Semi-colon delimited in-lists is a convention originating in
   * org.pentaho.di.core.Condition
   */
  public Object[] inListToTypedObjectArray( String fieldName, String value ) throws PushDownOptimizationException {
    String[] inList  = Const.splitString( value, ';', true );
    unescapeList( inList );
    ValueMetaInterface valueMeta = getValueMeta( fieldName );
    if ( valueMeta.isString() ) {
      // no type conversion necessary
      return inList;
    }
    return convertArrayToType( inList, valueMeta );
  }

  private void unescapeList( String[] inList ) {
    for ( int i = 0; i < inList.length; i++ ) {
      inList[ i ] = inList[ i ] == null ? null : inList[ i ].replace( "\\;", ";" );
    }
  }

  private Object[] convertArrayToType( String[] inList, ValueMetaInterface valueMeta ) throws PushDownOptimizationException {
    final int type = valueMeta.getType();
    Object[] objects = getTypedArray( type, inList.length );
    for ( int i = 0; i < inList.length; i++ ) {
      objects[i] = convertToType( valueMeta, ValueMetaInterface.TYPE_STRING, inList[i] );
    }
    return objects;
  }

  private Object[] getTypedArray( int type, int length ) throws PushDownOptimizationException {
    switch ( type ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return new Double[ length ];
      case ValueMetaInterface.TYPE_DATE:
        return new Date[ length ];
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return new Timestamp[ length ];
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new Boolean[ length ];
      case ValueMetaInterface.TYPE_INTEGER:
        return new Long[ length ];
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return new BigDecimal[ length ];
      default:
        throw new PushDownOptimizationException( "Cannot create an array of type code " + type );
    }
  }

  private Object convertToType( ValueMetaInterface valueMeta, int originalType, Object value )
    throws PushDownOptimizationException {
    try {
      final ValueMeta meta = new ValueMeta( null, originalType );
      meta.setConversionMask( valueMeta.getConversionMask() );
      return valueMeta.convertData( meta, value );
    } catch ( KettleValueException valueException ) {
      Object val = tryAnsiFormatConversion( valueMeta, originalType, value );
      if ( val == null ) {
        throw new PushDownOptimizationException( "Failed to convert type", valueException );
      }
      return val;
    }
  }

  private Object tryAnsiFormatConversion( ValueMetaInterface valueMeta, int originalType, Object value )
    throws PushDownOptimizationException {
    if ( valueMeta.getType() == ValueMetaInterface.TYPE_DATE ) {
      return tryConversionWithMask(
        valueMeta, originalType, value, ANSI_DATE_LITERAL );
    } else if ( valueMeta.getType() == ValueMetaInterface.TYPE_TIMESTAMP ) {
      return tryConversionWithMask(
        new ValueMetaTimestamp(), originalType, value, ANSI_TIMESTAMP_LITERAL );
    }
    return null;
  }

  private Object tryConversionWithMask(
    ValueMetaInterface valueMeta, int originalType, Object value, String mask )
    throws PushDownOptimizationException {
    ValueMeta originalTypeMeta = new ValueMeta( null, originalType );
    originalTypeMeta.setConversionMask( mask );
    try {
      return valueMeta.convertData( originalTypeMeta, value );
    } catch ( KettleValueException e ) {
      throw new PushDownOptimizationException( "Failed to convert type", e );
    }
  }

}
