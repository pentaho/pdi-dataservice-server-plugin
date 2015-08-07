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

package org.pentaho.di.trans.dataservice.ui.model;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by mburgess on 8/7/15.
 */
public class DataServiceTestModelTest {

  DataServiceTestModel model;

  @Before
  public void setUp() throws Exception {
    model = new DataServiceTestModel();
  }

  @Test
  public void testGetSetSql() throws Exception {
    assertNull( model.getSql() );
    model.setSql( "SELECT * FROM DUAL" );
    assertEquals( "SELECT * FROM DUAL", model.getSql() );
  }

  @Test
  public void testGetSetServiceTransLogChannel() throws Exception {
    assertNull( model.getServiceTransLogChannel() );
    LogChannelInterface mockChannel = mock( LogChannelInterface.class );
    model.setServiceTransLogChannel( mockChannel );
    assertEquals( mockChannel, model.getServiceTransLogChannel() );
  }

  @Test
  public void testGetSetGenTransLogChannel() throws Exception {
    assertNull( model.getGenTransLogChannel() );
    LogChannelInterface mockChannel = mock( LogChannelInterface.class );
    model.setGenTransLogChannel( mockChannel );
    assertEquals( mockChannel, model.getGenTransLogChannel() );
  }

  @Test
  public void testGetSetClearResultRows() throws Exception {
    List<Object[]> rows = model.getResultRows();
    assertNotNull( rows );
    assertTrue( rows.isEmpty() );
    Object[] row1 = { 3, "Hello" };
    Object[] row2 = { 5, "World!" };
    model.addResultRow( row1 );
    model.addResultRow( row2 );
    rows = model.getResultRows();
    assertEquals( 2, rows.size() );
    model.clearResultRows();
    rows = model.getResultRows();
    assertNotNull( rows );
    assertTrue( rows.isEmpty() );
  }


  @Test
  public void testGetLogLevel() throws Exception {
    assertEquals( model.getLogLevel(), DataServiceTestModel.DEFAULT_LOGLEVEL );
    model.setLogLevel( LogLevel.BASIC );
    assertEquals( model.getLogLevel(), LogLevel.BASIC );
  }

  @Test
  public void testGetAllLogLevels() throws Exception {
    LogLevel[] levels = LogLevel.values();
    List<String> levelValues = model.getAllLogLevels();
    assertNotNull( levelValues );
    assertEquals( levels.length, levelValues.size() );
    for ( LogLevel level : levels ) {
      assertTrue( levelValues.contains( level.getDescription() ) );
    }
  }

  @Test
  public void testGetSetMaxRows() throws Exception {
    // getMaxRows gives you the value at the index, where setMaxRows expects an index
    assertEquals( DataServiceTestModel.MAXROWS_CHOICES.get( 0 ).intValue(), model.getMaxRows() );
    model.setMaxRows( 1 );
    assertEquals( DataServiceTestModel.MAXROWS_CHOICES.get( 1 ).intValue(), model.getMaxRows() );
  }

  @Test( expected = ArrayIndexOutOfBoundsException.class )
  public void testGetMaxRowsException() throws Exception {
    model.setMaxRows( DataServiceTestModel.MAXROWS_CHOICES.size() + 1 );
    model.getMaxRows();
  }

  @Test
  public void testGetAllMaxRows() throws Exception {
    List<Integer> maxRowChoices = model.getAllMaxRows();
    assertNotNull( maxRowChoices );
    assertEquals( DataServiceTestModel.MAXROWS_CHOICES.size(), maxRowChoices.size() );
    for ( Integer choice : DataServiceTestModel.MAXROWS_CHOICES ) {
      assertTrue( maxRowChoices.contains( choice ) );
    }
  }

  @Test
  public void testGetSetAlertMessage() throws Exception {
    assertNull( model.getAlertMessage() );
    model.setAlertMessage( "red alert" );
    assertEquals( "red alert", model.getAlertMessage() );
  }

  @Test
  public void testGetSetResultRowMeta() throws Exception {
    assertNull( model.getResultRowMeta() );
    RowMetaInterface mockRowMeta = mock( RowMetaInterface.class );
    model.setResultRowMeta( mockRowMeta );
    assertEquals( mockRowMeta, model.getResultRowMeta() );
  }

  @Test
  public void testGetSetClearOptimizationImpactDescription() throws Exception {
    assertEquals( "\n[No Push Down Optimizations Defined]\n", model.getOptimizationImpactDescription() );

    OptimizationImpactInfo mockInfo = mock( OptimizationImpactInfo.class );
    when( mockInfo.getDescription() ).thenReturn( "my optimization" );
    model.addOptimizationImpact( mockInfo );
    assertEquals( "\nmy optimization\n- - - - - - - - - - - - - - - - - - - - - -\n\n",
      model.getOptimizationImpactDescription() );

    model.clearOptimizationImpact();
    assertEquals( "\n[No Push Down Optimizations Defined]\n", model.getOptimizationImpactDescription() );
  }

  @Test
  public void testGetSetExecuting() throws Exception {
    assertFalse( model.isExecuting() );
    model.setExecuting( true );
    assertTrue( model.isExecuting() );
  }
}
