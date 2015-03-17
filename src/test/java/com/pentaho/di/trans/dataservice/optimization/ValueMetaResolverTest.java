/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

import static com.mongodb.util.MyAsserts.assertTrue;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ValueMetaResolverTest  {

  @Test
  public void getValueMetaForIntField() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_INTEGER ) );
    assertEquals( ValueMetaInterface.TYPE_INTEGER,
      resolver.getValueMeta( "field" ).getType() );
  }

  @Test
  public void stringResolvedToLong() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_INTEGER ) );
    int originalType = ValueMetaInterface.TYPE_STRING;
    Object value = resolver.getTypedValue( "field", originalType, "101010" );
    assertTrue( value instanceof Long );
    assertTrue( value.equals( new Long( 101010 ) ) );
  }

  @Test
  public void getMixedValues() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta(
        new String[] { "field1", "field2", "field3" },
        new int[] { ValueMetaInterface.TYPE_INTEGER,
          ValueMetaInterface.TYPE_BIGNUMBER,
          ValueMetaInterface.TYPE_BOOLEAN
        } ) );
    assertEquals( new Long( 123 ),
      resolver.getTypedValue( "field1", ValueMetaInterface.TYPE_INTEGER, 123l ) );
    assertEquals( new BigDecimal( 123 ),
      resolver.getTypedValue( "field2", ValueMetaInterface.TYPE_INTEGER, 123l ) );
    assertEquals( new Boolean( true ),
      resolver.getTypedValue( "field3", ValueMetaInterface.TYPE_INTEGER, 1l ) );

    try {
      resolver.getTypedValue( "fieldNonExistent", ValueMetaInterface.TYPE_STRING, "blah" );
    } catch ( KettleException e ) {
      return;
    }
    fail( "Expected exception when resolving a non-existent field" );
  }

  @Test
  public void stringResolvedToDate() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_DATE ) );
    int originalType = ValueMetaInterface.TYPE_STRING;
    final String convertedDateStr = "2014-10-10";
    Object value = resolver.getTypedValue( "field", originalType, convertedDateStr );
    assertEquals( convertedDateStr, new SimpleDateFormat( "yyyy-MM-dd" ).format( value ) );
  }

  @Test
  public void stringResolvedToTimestamp() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_TIMESTAMP ) );
    int originalType = ValueMetaInterface.TYPE_STRING;
    final String convertedTimestampStr = "2014-10-10 12:24:32.012";
    Object value = resolver.getTypedValue( "field", originalType, convertedTimestampStr );
    assertEquals( convertedTimestampStr,
      new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format( value ) );
  }

  @Test
  public void inListOfStringsResolveToStringArray() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_STRING ) );
    Object[] values = resolver.inListToTypedObjectArray( "field", "f\\;oo;bar;baz" );
    assertThat( values, equalTo( new Object[]{"f;oo", "bar", "baz"} ) );
  }

  @Test
  public void inListOfIntsResolveToLongArray() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_INTEGER ) );
    Object[] values = resolver.inListToTypedObjectArray( "field", "1;2;3" );
    assertThat( values, equalTo( new Object[]{1l, 2l, 3l} ) );
  }

  @Test
  public void inListOfStringsResolveToDateArray() throws KettleException {
    ValueMetaResolver resolver = new ValueMetaResolver(
      getTestRowMeta( ValueMetaInterface.TYPE_DATE ) );
    Object[] values = resolver.inListToTypedObjectArray( "field", "1997-04-12;2001-01-01;2008-9-04" );
    String[] expected = new String[]{"1997-04-12", "2001-01-01", "2008-09-04"};
    for ( int i = 0; i < values.length; i++ ) {
      assertEquals( expected[ i ],
        new SimpleDateFormat( "yyyy-MM-dd" ).format( values[i] ) );
    }
  }

  private RowMeta getTestRowMeta( int type ) {
    return getTestRowMeta( new String[]{ "field" }, new int[] { type } );
  }

  private RowMeta getTestRowMeta( String[] fieldNames, int[] types ) {
    assert fieldNames.length == types.length;
    RowMeta rowMeta = new RowMeta();
    for ( int i = 0; i < fieldNames.length; i++ ) {
      int type = types[ i ];
      String fieldName = fieldNames[ i ];
      rowMeta.addValueMeta( new ValueMeta(
        fieldName, type ) );
    }
    return rowMeta;
  }

}
