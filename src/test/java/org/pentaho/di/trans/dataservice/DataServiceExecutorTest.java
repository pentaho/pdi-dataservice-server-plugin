/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
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
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.metastore.api.IMetaStore;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.testing.answers.ReturnsSelf.RETURNS_SELF;

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceExecutorTest extends BaseTest {

  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";
  public static final String CONTAINER_ID = "12345";
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans serviceTrans;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans genTrans;
  @Mock SqlTransGenerator sqlTransGenerator;
  @Mock BiConsumer<String, TransMeta> mutator;
  @Mock TransMeta serviceTransMeta;

  @Before
  public void setUp() throws Exception {
    doAnswer( RETURNS_SELF ).when( transMeta ).realClone( anyBoolean() );
    doAnswer( RETURNS_SELF ).when( transMeta ).clone();

    when( serviceTrans.getContainerObjectId() ).thenReturn( CONTAINER_ID );
    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP_NAME );
    when( sqlTransGenerator.getResultStepName() ).thenReturn( RESULT_STEP_NAME );
  }

  @Test
  public void testLogging() throws Exception {
    when( serviceTrans.getTransMeta() ).thenReturn( transMeta );
    TransMeta genTransMeta = mock( TransMeta.class );
    when( genTrans.getTransMeta() ).thenReturn( genTransMeta );

    new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context ).
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

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), dataService, context ).
      serviceTrans( transMeta ).
      prepareExecution( false ).
      build();

    Condition condition = executor.getSql().getWhereCondition().getCondition();

    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set( 2014, Calendar.DECEMBER, 5 );

    assertThat( condition.evaluate( rowMeta, new Object[] { "value", 2L, calendar.getTime() } ), is( true ) );
    assertThat( condition.evaluate( rowMeta, new Object[] { "value", 2L, new Date() } ), is( false ) );
  }

  @Test
  public void testExecuteQuery() throws Exception {
    SQL sql = new SQL( "SELECT * FROM " + DATA_SERVICE_NAME );
    StepInterface serviceStep = serviceTrans.findRunThread( DATA_SERVICE_STEP );
    StepInterface resultStep = genTrans.findRunThread( RESULT_STEP_NAME );

    when( serviceTrans.getTransMeta().listParameters() ).thenReturn( new String[0] );
    when( genTrans.isRunning() ).thenAnswer( new Answer<Boolean>() {
      private int count;
      @Override public Boolean answer( final InvocationOnMock invocationOnMock ) throws Throwable {
        count++;
        return count % 2 != 0;
      }
    } );

    PushDownOptimizationMeta optimization = mock( PushDownOptimizationMeta.class );
    when( optimization.isEnabled() ).thenReturn( true );
    dataService.getPushDownOptimizationMeta().add( optimization );

    IMetaStore metastore = mock( IMetaStore.class );
    DataServiceExecutor executor = new DataServiceExecutor.Builder( sql, dataService, context ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransGenerator ).
      genTrans( genTrans ).
      metastore( metastore ).
      build();

    ArgumentCaptor<String> objectIds = ArgumentCaptor.forClass( String.class );
    verify( serviceTrans ).setContainerObjectId( objectIds.capture() );
    when( serviceTrans.getContainerObjectId() ).thenReturn( objectIds.getValue() );
    verify( genTrans ).setContainerObjectId( objectIds.capture() );
    when( genTrans.getContainerObjectId() ).thenReturn( objectIds.getValue() );
    verify( serviceTrans ).setMetaStore( metastore );
    verify( genTrans ).setMetaStore( metastore );

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
    RowMetaInterface rowMeta = genTrans.getTransMeta().getStepFields( RESULT_STEP_NAME );
    Object[] data;
    for ( int i = 0; i < 50; i++ ) {
      data = new Object[] { i };

      Object[] dataClone = { i };
      when( rowMeta.cloneRow( data ) ).thenReturn( dataClone );
      serviceRowListener.rowWrittenEvent( rowMeta, data );
      verify( sqlTransRowProducer )
        .putRowWait( same( rowMeta ), and( eq( dataClone ), not( same( data ) ) ), any( Long.class ), any( TimeUnit.class ) );
      verify( rowMeta ).cloneRow( data );
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
    transListenerCaptor.getValue().transFinished( genTrans );

    InOrder writeRows = inOrder( rowMeta );
    ArgumentCaptor<DataOutputStream> streamCaptor = ArgumentCaptor.forClass( DataOutputStream.class );
    writeRows.verify( rowMeta ).writeMeta( streamCaptor.capture() );
    DataOutputStream dataOutputStream = streamCaptor.getValue();
    writeRows.verify( rowMeta, times( 50 ) ).writeData( same( dataOutputStream ), argThat( arrayWithSize( 1 ) ) );
    writeRows.verifyNoMoreInteractions();

    executor.waitUntilFinished();
    verify( serviceTrans ).waitUntilFinished();
    verify( genTrans ).waitUntilFinished();
  }

  @Test
  public void testQueryWithParams() throws Exception {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE PARAMETER('foo') = 'bar' AND PARAMETER('baz') = 'bop'";

    final SQL theSql = new SQL( sql );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( theSql, dataService, context ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransGenerator ).
      genTrans( genTrans ).
      parameters( ImmutableMap.of( "BUILD_PARAM", "TRUE" ) ).
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

    assertThat( executor.getParameters(), equalTo( (Map<String, String>) ImmutableMap.of(
      "baz", "bop",
      "foo", "bar",
      "BUILD_PARAM", "TRUE"
    ) ) );

    // Late parameter modification is okay
    executor.getParameters().put( "AFTER_BUILD", "TRUE" );

    // Parameters should not be set on the trans until execute
    verify( serviceTrans, never() ).setParameterValue( anyString(), anyString() );

    executor.executeQuery();
    // verify that the parameter values were correctly extracted from the WHERE and applied
    verify( serviceTrans ).setParameterValue( "foo", "bar" );
    verify( serviceTrans ).setParameterValue( "baz", "bop" );
    verify( serviceTrans ).setParameterValue( "BUILD_PARAM", "TRUE" );
    verify( serviceTrans ).setParameterValue( "AFTER_BUILD", "TRUE" );
  }

  @Test
  public void testNullNotNullKeywords() throws Exception {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME + " WHERE column1 IS NOT NULL AND column2 IS NULL";

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), dataService, context ).
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

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), dataService, context ).
      serviceTrans( new Trans( transMeta ) ).
      prepareExecution( false ).
      build();

    executor.getSql().getWhereCondition().getCondition().evaluate( rowMeta, new Object[] { "value".getBytes() } );
  }

  @Test
  public void testBuilderFailsOnNulls() {
    try {
      new DataServiceExecutor.Builder( null, mock( DataServiceMeta.class ), context );
      fail( "Should fail when SQL is null" );
    } catch ( NullPointerException npe ) {
      // Expected exception
    }

    try {
      new DataServiceExecutor.Builder( mock( SQL.class ), null, context );
      fail( "Should fail when service is null" );
    } catch ( NullPointerException npe ) {
      // Expected exception
    }
  }

  @Test
  public void testStop() throws KettleException {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME;

    when( serviceTrans.isRunning() ).thenReturn( true );
    when( genTrans.isRunning() ).thenReturn( true );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), dataService, context ).
        serviceTrans( serviceTrans ).
        sqlTransGenerator( sqlTransGenerator ).
        genTrans( genTrans ).
        build();

    executor.stop();

    verify( serviceTrans ).stopAll();
    verify( genTrans ).stopAll();
  }

  @Test
  public void testIsComplete() throws KettleException {
    String sql = "SELECT * FROM " + DATA_SERVICE_NAME;

    when( genTrans.isStopped() ).thenReturn( true );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), dataService, context ).
        serviceTrans( serviceTrans ).
        sqlTransGenerator( sqlTransGenerator ).
        genTrans( genTrans ).
        build();

    assertTrue( executor.isStopped() );
  }

  @Test
  public void testExecuteConcurrentModification() throws Exception {

    String sql = "SELECT * FROM " + DATA_SERVICE_NAME;
    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), dataService, context ).
      prepareExecution( false ).
      sqlTransGenerator( sqlTransGenerator ).
      serviceTrans( serviceTrans ).
      genTrans( genTrans ).
      build();

    final DataServiceExecutor.ExecutionPoint stage = DataServiceExecutor.ExecutionPoint.OPTIMIZE;
    final ListMultimap<DataServiceExecutor.ExecutionPoint, Runnable> listenerMap = executor.getListenerMap();
    final Runnable task = new Runnable() {
      @Override public void run() {
        // Remove itself on run
        assertTrue( listenerMap.remove( stage, this ) );
      }
    };

    listenerMap.put( stage, task );
    executor.executeQuery();

    // Note the error reported to logs
    verify( genTrans.getLogChannel() ).logError( anyString(),
      eq( stage ), eq( ImmutableList.of( task ) ), eq( ImmutableList.of() )
    );
  }

  @Test
  public void testDynamicLimit() throws Exception {
    final String limitProp = "det.dataservice.dynamic.limit";
    boolean userDef = dataService.isUserDefined();
    try {
      System.setProperty( limitProp, "2" );

      dataService.setUserDefined( false );
      DataServiceExecutor executor =
        new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context )
          .serviceTrans( serviceTrans )
          .genTrans( genTrans )
          .build();
      assertEquals( 2, executor.getServiceRowLimit() );
      dataService.setUserDefined( true );
      System.setProperty( limitProp, "2" );
      executor =
        new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context )
          .serviceTrans( serviceTrans )
          .genTrans( genTrans )
          .build();
      assertEquals( 0, executor.getServiceRowLimit() );
    } finally {
      dataService.setUserDefined( userDef );
      System.getProperties().remove( limitProp );
    }
  }

  @Test
  public void testDynamicLimitDefault() throws Exception {
    final String limitProp = "det.dataservice.dynamic.limit";
    final int defaultLimit = 50000;
    boolean userDef = dataService.isUserDefined();
    try {
      System.getProperties().remove( limitProp );
      dataService.setUserDefined( false );
      DataServiceExecutor executor =
        new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context )
          .serviceTrans( serviceTrans )
          .genTrans( genTrans )
          .build();
      assertEquals( defaultLimit, executor.getServiceRowLimit() );

      System.setProperty( limitProp, "baah" );
      executor =
          new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context )
            .serviceTrans( serviceTrans )
            .genTrans( genTrans )
            .build();
      assertEquals( defaultLimit, executor.getServiceRowLimit() );
      verify( logChannel ).logError( anyString() );
    } finally {
      dataService.setUserDefined( userDef );
      System.getProperties().remove( limitProp );
    }
  }

  @Test
  public void testMutatorGetsCalled() throws Exception {
    when( serviceTransMeta.realClone( false ) ).thenReturn( serviceTransMeta );
    when( serviceTransMeta.listVariables() ).thenReturn( new String[]{} );
    when( serviceTransMeta.listParameters() ).thenReturn( new String[]{} );
      new DataServiceExecutor.Builder( new SQL( "SELECT * FROM " + DATA_SERVICE_NAME ), dataService, context )
        .serviceTransMutator( mutator )
        .serviceTrans( serviceTransMeta );
    Mockito.verify( mutator ).accept( dataService.getStepname(), serviceTransMeta );
  }
}
