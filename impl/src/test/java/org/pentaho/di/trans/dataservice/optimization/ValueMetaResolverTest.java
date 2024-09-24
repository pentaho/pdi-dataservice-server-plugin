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

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.collect.ObjectArrays;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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
    assertTrue( value.equals( 101010L ) );
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
    assertEquals( 123L,
      resolver.getTypedValue( "field1", ValueMetaInterface.TYPE_INTEGER, 123L ) );
    assertEquals( new BigDecimal( 123 ),
      resolver.getTypedValue( "field2", ValueMetaInterface.TYPE_INTEGER, 123L ) );
    assertEquals( true,
      resolver.getTypedValue( "field3", ValueMetaInterface.TYPE_INTEGER, 1L ) );

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
    assertThat( values, arrayContaining( (Object) 1L, 2L, 3L ) );

    final ValueMetaInterface valueMeta = resolver.getValueMeta( "field" );

    String[] stringValues = ObjectArrays.newArray( String.class, values.length );
    for ( int i = 0; i < values.length; i++ ) {
      stringValues[i] = valueMeta.getString( values[i] );
    }

    assertThat( stringValues, arrayContaining( "1", "2", "3" ) );
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
