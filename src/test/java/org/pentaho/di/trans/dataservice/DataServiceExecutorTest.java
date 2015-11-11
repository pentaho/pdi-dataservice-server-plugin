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

package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.testing.answers.ReturnsSelf.RETURNS_SELF;

public class DataServiceExecutorTest extends BaseTest {

  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";

  @Before
  public void setUp() throws Exception {
    doAnswer( RETURNS_SELF ).when( transMeta ).realClone( anyBoolean() );
    doAnswer( RETURNS_SELF ).when( transMeta ).clone();
  }

  @Test
  public void testLogging() throws Exception {
    Trans serviceTrans = mock( Trans.class );
    when( serviceTrans.getTransMeta() ).thenReturn( transMeta );
    Trans genTrans = mock( Trans.class );
    TransMeta genTransMeta = mock( TransMeta.class );
    when( genTrans.getTransMeta() ).thenReturn( genTransMeta );

    new DataServiceExecutor.Builder( new SQL( "SELECT foo FROM " + DATA_SERVICE_NAME ), dataService ).
      serviceTrans( serviceTrans ).
      genTrans( genTrans ).
      prepareExecution( false ).
      logLevel( LogLevel.DETAILED ).
      build();

    verify( serviceTrans ).setLogLevel( LogLevel.DETAILED );
    verify( transMeta ).setLogLevel( LogLevel.DETAILED );
    verify( genTrans ).setLogLevel( LogLevel.DETAILED );
    verify( genTransMeta ).setLogLevel( LogLevel.DETAILED );
  }

  @Test
  public void testConditionResolution() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "aString" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "anInt" ) );
    rowMeta.addValueMeta( new ValueMetaDate( "aDate" ) );

    String query = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE anInt = 2 AND aDate IN ('2014-12-05','2008-01-01')";

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenReturn( rowMeta );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), dataService ).
      serviceTrans( transMeta ).
      prepareExecution( false ).
      build();

    Condition condition = executor.getSql().getWhereCondition().getCondition();

    assertEquals( condition.getCondition( 0 ).getRightExact().getValueMeta().getType(),
      ValueMetaInterface.TYPE_INTEGER );
    String dateList = condition.getCondition( 1 ).getRightExactString();
    for ( Object date : new ValueMetaResolver( rowMeta ).inListToTypedObjectArray( "aDate", dateList ) ) {
      assertThat( date, instanceOf( Date.class ) );
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SQL sql = new SQL( "SELECT * FROM " + DATA_SERVICE_NAME );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();
    StepInterface serviceStep = serviceTrans.findRunThread( DATA_SERVICE_STEP );
    StepInterface resultStep = genTrans.findRunThread( RESULT_STEP_NAME );

    when( serviceTrans.getTransMeta().listParameters() ).thenReturn( new String[0] );

    PushDownOptimizationMeta optimization = mock( PushDownOptimizationMeta.class );
    when( optimization.isEnabled() ).thenReturn( true );
    dataService.getPushDownOptimizationMeta().add( optimization );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( sql, dataService ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransGenerator ).
      genTrans( genTrans ).
      build();

    ArgumentCaptor<String> objectIds = ArgumentCaptor.forClass( String.class );
    verify( serviceTrans ).setContainerObjectId( objectIds.capture() );
    when( serviceTrans.getContainerObjectId() ).thenReturn( objectIds.getValue() );
    verify( genTrans ).setContainerObjectId( objectIds.capture() );
    when( genTrans.getContainerObjectId() ).thenReturn( objectIds.getValue() );

    RowProducer sqlTransRowProducer = mock( RowProducer.class );
    when( genTrans.addRowProducer( INJECTOR_STEP_NAME, 0 ) ).thenReturn( sqlTransRowProducer );

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // Start Execution
    executor.executeQuery( new DataOutputStream( outputStream ) );

    // Check header was written
    assertThat( outputStream.size(), greaterThan( 0 ) );
    outputStream.reset();

    InOrder genTransStartup = inOrder( genTrans, resultStep );
    InOrder serviceTransStartup = inOrder( optimization, serviceTrans, serviceStep );
    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> resultStepListener = ArgumentCaptor.forClass( StepListener.class );
    ArgumentCaptor<TransListener> transListenerCaptor = ArgumentCaptor.forClass( TransListener.class );

    genTransStartup.verify( genTrans ).addTransListener( transListenerCaptor.capture() );
    genTransStartup.verify( genTrans ).addRowProducer( INJECTOR_STEP_NAME, 0 );
    genTransStartup.verify( resultStep ).addStepListener( resultStepListener.capture() );
    genTransStartup.verify( resultStep ).addRowListener( listenerArgumentCaptor.capture() );
    RowListener clientRowListener = listenerArgumentCaptor.getValue();
    genTransStartup.verify( genTrans ).startThreads();

    serviceTransStartup.verify( optimization ).activate( executor );
    serviceTransStartup.verify( serviceStep ).addRowListener( listenerArgumentCaptor.capture() );
    serviceTransStartup.verify( serviceTrans ).startThreads();

    // Verify linkage
    RowListener serviceRowListener = listenerArgumentCaptor.getValue();
    assertNotNull( serviceRowListener );

    // Push row from service to sql Trans
    RowMeta rowMeta = mock( RowMeta.class );
    Object[] data;
    for ( int i = 0; i < 50; i++ ) {
      data = new Object[] { i };

      serviceRowListener.rowWrittenEvent( rowMeta, data );
      verify( sqlTransRowProducer ).putRowWait( same( rowMeta ), eq( data ), any( Long.class ), any( TimeUnit.class ) );
    }

    doReturn( true ).when( serviceTrans ).isRunning();
    resultStepListener.getValue().stepFinished( genTrans, resultStep.getStepMeta(), resultStep );
    verify( serviceTrans ).stopAll();

    // Verify Service Trans finished
    ArgumentCaptor<StepListener> serviceStepListener = ArgumentCaptor.forClass( StepListener.class );
    verify( serviceStep ).addStepListener( serviceStepListener.capture() );
    serviceStepListener.getValue().stepFinished( serviceTrans, serviceStep.getStepMeta(), serviceStep );
    verify( sqlTransRowProducer ).finished();

    // Push row from service to sql Trans
    for ( int i = 0; i < 50; i++ ) {
      Object[] row = { i };
      clientRowListener.rowWrittenEvent( rowMeta, row );
    }

    InOrder writeRows = inOrder( rowMeta );
    ArgumentCaptor<DataOutputStream> streamCaptor = ArgumentCaptor.forClass( DataOutputStream.class );
    writeRows.verify( rowMeta ).writeMeta( streamCaptor.capture() );
    DataOutputStream dataOutputStream = streamCaptor.getValue();
    writeRows.verify( rowMeta, times( 50 ) ).writeData( same( dataOutputStream ), argThat( arrayWithSize( 1 ) ) );

    // Gen trans finished, assert no more data is written
    outputStream.reset();
    transListenerCaptor.getValue().transFinished( genTrans );
    assertThat( outputStream.size(), equalTo( 0 ) );

    executor.waitUntilFinished();
    verify( serviceTrans ).waitUntilFinished();
    verify( genTrans ).waitUntilFinished();
  }

  @Test
  public void testQueryWithParams() throws Exception {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE PARAMETER('foo') = 'bar' AND PARAMETER('baz') = 'bop'";
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();

    final SQL theSql = new SQL( sql );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( theSql, dataService ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransGenerator ).
      genTrans( genTrans ).
      build();

    List<Condition> conditions = theSql.getWhereCondition().getCondition().getChildren();

    assertEquals( 2, conditions.size() );
    for ( Condition condition : conditions ) {
      // verifies that each of the parameter conditions have their left and right valuename
      // set to null after executor initialization.  This prevents failure due to non-existent
      // fieldnames being present.
      assertNull( condition.getLeftValuename() );
      assertNull( condition.getRightValuename() );
    }
    // verify that the parameter values were correctly extracted from the WHERE
    verify( executor.getServiceTransMeta() ).setParameterValue( "foo", "bar" );
    verify( executor.getServiceTransMeta() ).setParameterValue( "baz", "bop" );

    Map<String, String> expectedParams = new HashMap<String, String>();
    expectedParams.put( "baz", "bop" );
    expectedParams.put( "foo", "bar" );

    assertEquals( expectedParams, executor.getParameters() );
  }

  @Test
  public void testNullNotNullKeywords() throws Exception {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE column1 IS NOT NULL AND column2 IS NULL";

    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), dataService ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransGenerator ).
      genTrans( genTrans ).
      build();

    Condition condition = executor.getSql().getWhereCondition().getCondition();

    Condition condition1 = condition.getCondition( 0 );
    Condition condition2 = condition.getCondition( 1 );

    assertEquals( "column1", condition1.getLeftValuename() );
    assertNull( condition1.getRightExact() );
    assertEquals( Condition.FUNC_NOT_NULL, condition1.getFunction() );

    assertEquals( "column2", condition2.getLeftValuename() );
    assertNull( condition2.getRightExact() );
    assertEquals( Condition.FUNC_NULL, condition2.getFunction() );
  }

  @Test
  public void testWithLazyConversion() throws Exception {
    RowMeta rowMeta = new RowMeta();
    ValueMetaInterface vm = new ValueMetaString( "aBinaryStoredString" );
    vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
    vm.setStorageMetadata( new ValueMetaString() );
    rowMeta.addValueMeta( vm );

    String query = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE aBinaryStoredString = 'value'";

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenReturn( rowMeta );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), dataService ).
      serviceTrans( new Trans( transMeta ) ).
      prepareExecution( false ).
      build();

    executor.getSql().getWhereCondition().getCondition().evaluate( rowMeta, new Object[] { "value".getBytes() } );
  }

  private SqlTransGenerator mockSqlTransGenerator() {
    SqlTransGenerator sqlTransGenerator = mock( SqlTransGenerator.class );
    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP_NAME );
    when( sqlTransGenerator.getResultStepName() ).thenReturn( RESULT_STEP_NAME );
    return sqlTransGenerator;
  }

  @Test
  public void testBuilderFailsOnNulls() {
    try {
      new DataServiceExecutor.Builder( null, mock( DataServiceMeta.class ) );
      fail( "Should fail when SQL is null" );
    } catch ( NullPointerException npe ) {
      // Expected exception
    }

    try {
      new DataServiceExecutor.Builder( mock( SQL.class ), null );
      fail( "Should fail when service is null" );
    } catch ( NullPointerException npe ) {
      // Expected exception
    }
  }
}
