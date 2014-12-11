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

package com.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.pentaho.di.core.row.ValueMetaInterface.*;

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

  public ValueMetaResolver( RowMetaInterface rowMeta ) {
    Map<String, ValueMetaInterface> tempFieldNameValueMetaMap =
      new HashMap<String, ValueMetaInterface>();
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      tempFieldNameValueMetaMap.put( rowMeta.getFieldNames()[i],
        rowMeta.getValueMeta( i ) );
    }
    fieldNameValueMetaMap = Collections.unmodifiableMap( tempFieldNameValueMetaMap );
  }

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
      objects[i] = convertToType( valueMeta, TYPE_STRING, inList[i] );
    }
    return objects;
  }

  private Object[] getTypedArray( int type, int length ) throws PushDownOptimizationException {
    switch ( type ) {
      case TYPE_NUMBER:
        return new Double[ length ];
      case TYPE_DATE:
        return new Date[ length ];
      case TYPE_TIMESTAMP:
        return new Timestamp[ length ];
      case TYPE_BOOLEAN:
        return new Boolean[ length ];
      case TYPE_INTEGER:
        return new Long[ length ];
      case TYPE_BIGNUMBER:
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
    if ( valueMeta.getType() == TYPE_DATE ) {
      return tryConversionWithMask(
        valueMeta, originalType, value, ANSI_DATE_LITERAL );
    } else if ( valueMeta.getType() == TYPE_TIMESTAMP ) {
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
