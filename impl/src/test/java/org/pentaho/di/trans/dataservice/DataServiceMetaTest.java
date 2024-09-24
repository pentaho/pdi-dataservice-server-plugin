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

package org.pentaho.di.trans.dataservice;

import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class DataServiceMetaTest extends BaseTest {

  private Pattern patternSaveUserDefined = Pattern.compile( "<key>is_user_defined<\\/key>\\s*<value>Y<\\/value>" );
  private Pattern patternSaveTransient   = Pattern.compile( "<key>is_user_defined<\\/key>\\s*<value>N<\\/value>" );
  private Pattern patternSaveStreaming   = Pattern.compile( "<key>streaming<\\/key>\\s*<value>Y<\\/value>" );
  private Pattern patternStepName        = Pattern.compile( "<key>step_name<\\/key>\\s*<value>MockStepName<\\/value>" );
  private Pattern patternRowLimit        = Pattern.compile( "<key>row_limit<\\/key>\\s*<value>-1<\\/value>" );
  private Pattern patternTimeLimit        = Pattern.compile( "<key>time_limit<\\/key>\\s*<value>999<\\/value>" );

  private String MOCK_NAME = "MockName";
  private String MOCK_STEP_NAME = "MockStepName";
  private Integer MOCK_ROW_LIMIT = -1;
  private long MOCK_TIME_LIMIT = 999;

  @Mock TransMeta serviceTransMeta;
  @Mock List<PushDownOptimizationMeta> pushDownOptimizations;

  @Test
  public void testName() {
    assertNotEquals( MOCK_NAME, dataService.getName() );
    dataService.setName( MOCK_NAME );
    assertEquals( MOCK_NAME, dataService.getName() );
  }

  @Test
  public void testStepName() {
    try {
      dataService.setStepname( MOCK_STEP_NAME );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.DATA_SERVICE_TRANSFORMATION_STEP_NAME,
        ( dataService.getStepname() ) );
      String xml = transMeta.getXML();
      assertTrue( patternStepName.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testRowLimit() {
    try {
      dataService.setRowLimit( MOCK_ROW_LIMIT );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.ROW_LIMIT,
        ( String.valueOf( dataService.getRowLimit() ) ) );
      String xml = transMeta.getXML();
      assertTrue( patternRowLimit.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testTimeLimit() {
    try {
      dataService.setTimeLimit( MOCK_TIME_LIMIT );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.TIME_LIMIT,
        ( String.valueOf( dataService.getTimeLimit() ) ) );
      String xml = transMeta.getXML();
      assertTrue( patternTimeLimit.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testSaveUserDefined() {
    try {
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.IS_USER_DEFINED,
          ( dataService.isUserDefined() ? "Y" : "N" ) );
      String xml = transMeta.getXML();
      assertTrue( patternSaveUserDefined.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testSaveTransient() {
    try {
      dataService.setUserDefined( false );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.IS_USER_DEFINED,
          ( dataService.isUserDefined() ? "Y" : "N" ) );
      String xml = transMeta.getXML();
      assertTrue( patternSaveTransient.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testSaveStreaming() {
    try {
      dataService.setStreaming( true );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.IS_STREAMING,
        ( dataService.isStreaming() ? "Y" : "N" ) );
      String xml = transMeta.getXML();
      assertTrue( patternSaveStreaming.matcher( xml ).find() );
    } catch ( Exception ex ) {
      fail();
    }
  }

  @Test
  public void testServiceTrans() {
    assertNotEquals( serviceTransMeta, dataService.getServiceTrans() );
    dataService.setServiceTrans( serviceTransMeta );
    assertEquals( serviceTransMeta, dataService.getServiceTrans() );
  }

  @Test
  public void testPushDownOptimizations() {
    assertNotEquals( pushDownOptimizations, dataService.getPushDownOptimizationMeta() );
    dataService.setPushDownOptimizationMeta( pushDownOptimizations );
    assertEquals( pushDownOptimizations, dataService.getPushDownOptimizationMeta() );
  }

  @Test
  public void testToString() {
    String result = "DataServiceMeta{name=DataService, serviceTrans=/DataService, stepname=Data Service Step,"
      + " userDefined=true, streaming=true, rowLimit=100, timeLimit=200,"
      + " pushDownOptimizationMeta=pushDownOptimizations}";

    dataService.setStreaming( true );
    dataService.setRowLimit( 100 );
    dataService.setTimeLimit( 200L );
    dataService.setPushDownOptimizationMeta( pushDownOptimizations );

    assertEquals( result, dataService.toString() );
  }
}
