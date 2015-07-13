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

import com.google.common.collect.Lists;
<<<<<<< HEAD:src/test/java/com/pentaho/di/trans/dataservice/DataServiceExecutorTest.java
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.junit.BeforeClass;
=======
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
>>>>>>> [BACKLOG-3775] Moved files from com to org package:src/test/java/org/pentaho/di/trans/dataservice/DataServiceExecutorTest.java
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceExecutorTest {

  public static final String SERVICE_NAME = "serviceName";
  public static final String SERVICE_STEP_NAME = "Service Step";
  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";

  @BeforeClass
  public static void init() throws KettleException {
    KettleEnvironment.init();
  }

  @Test
  public void testCreateExecutorByObjectId() throws Exception {
    String query = "SELECT field1, field2 FROM " + SERVICE_NAME;

    DataServiceMeta service = createDataServiceMeta();
    ObjectId transId = new StringObjectId( UUID.randomUUID().toString() );
    service.setTransObjectId( transId.getId() );

    TransMeta trans = mockTransMeta();

    Repository repository = mockRepository( transId, trans );

    List<DataServiceMeta> services = createServicesList( service );
    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), services ).
        lookupServiceTrans( repository ).
        normalizeConditions( false ).
        prepareExecution( false ).
        build();

    // Verify execution prep
    verify( trans, never() ).setName( anyString() );
    assertThat( executor.getServiceTransMeta(), sameInstance( trans.clone() ) );
    assertNotNull( executor.getGenTransMeta() );
    assertNotNull( "Service Trans not created", executor.getServiceTrans() );
    assertNotNull( "SQL Trans not created", executor.getGenTrans() );

    // Verify Properties
    assertEquals( SERVICE_NAME, executor.getServiceName() );
    assertNotNull( executor.getSql() );
    assertNotNull( executor.getResultStepName() );
  }

  @Test
  public void testLogging() throws Exception {
    DataServiceMeta serviceMeta = createDataServiceMeta();
    Trans serviceTrans = mock( Trans.class );
    TransMeta serviceTransMeta = mockTransMeta();
    when( serviceTrans.getTransMeta() ).thenReturn( serviceTransMeta );
    Trans genTrans = mock( Trans.class );
    TransMeta genTransMeta = mock( TransMeta.class );
    when( genTrans.getTransMeta() ).thenReturn( genTransMeta );

    new DataServiceExecutor.Builder( new SQL( "SELECT foo FROM bar" ), serviceMeta ).
      serviceTrans( serviceTrans ).
      genTrans( genTrans ).
      prepareExecution( false ).
      logLevel( LogLevel.DETAILED ).
      build();

    verify( serviceTrans ).setLogLevel( LogLevel.DETAILED );
    verify( serviceTransMeta ).setLogLevel( LogLevel.DETAILED );
    verify( genTrans ).setLogLevel( LogLevel.DETAILED );
    verify( genTransMeta ).setLogLevel( LogLevel.DETAILED );
  }

  @Test
  public void testConditionResolution() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "aString", ValueMeta.TYPE_STRING ) );
    rowMeta.addValueMeta( new ValueMeta( "anInt", ValueMeta.TYPE_INTEGER ) );
    rowMeta.addValueMeta( new ValueMeta( "aDate", ValueMeta.TYPE_DATE ) );

    String query = "SELECT * FROM " + SERVICE_NAME + " WHERE anInt = 2 AND aDate IN ('2014-12-05','2008-01-01')";

    DataServiceMeta service = createDataServiceMeta();
    TransMeta transMeta = mockTransMeta();
    when( transMeta.getStepFields( SERVICE_STEP_NAME ) ).thenReturn( rowMeta );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), service ).
        serviceTrans( transMeta ).
        prepareExecution( false ).
        build();

    Condition condition = executor.getSql().getWhereCondition().getCondition();

    assertEquals( condition.getCondition( 0 ).getRightExact().getValueMeta().getType(), ValueMeta.TYPE_INTEGER );
    String dateList = condition.getCondition( 1 ).getRightExactString();
    for ( Object date : new ValueMetaResolver( rowMeta ).inListToTypedObjectArray( "aDate", dateList ) ) {
      assertThat( date, instanceOf( Date.class ) );
    }
  }

  @Test
  public void testDual() throws Exception {
    ArrayList<String> queries = Lists.newArrayList( "Select 1,2,3 from DUAL", "Select 1,2,3" );
    List<DataServiceMeta> services = Collections.emptyList();

    for ( String query : queries ) {
      Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
      SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();
      RowListener clientRowListener = mock( RowListener.class );

      DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), services ).
        lookupServiceTrans( mock( Repository.class ) ).
        sqlTransGenerator( sqlTransGenerator ).
        genTrans( genTrans ).
        build();

      // Verify execution prep
      assertNull( executor.getServiceTrans() );
      assertNull( executor.getServiceTransMeta() );
      assertNotNull( executor.getGenTransMeta() );
      assertNotNull( executor.getGenTrans() );
      assertTrue( executor.isDual() );

      // Start Execution
      executor.executeQuery( clientRowListener );

      InOrder startup = inOrder( genTrans, genTrans.findRunThread( RESULT_STEP_NAME ) );

      startup.verify( genTrans ).prepareExecution( null );
      startup.verify( genTrans.findRunThread( RESULT_STEP_NAME ) ).addRowListener( clientRowListener );
      startup.verify( genTrans ).startThreads();
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {

    DataServiceMeta service = createDataServiceMeta();
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SQL sql = mock( SQL.class );
    when( sql.getServiceName() ).thenReturn( "test_service" );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();
    StepInterface serviceStep = serviceTrans.findRunThread( SERVICE_STEP_NAME );
    StepInterface resultStep = genTrans.findRunThread( RESULT_STEP_NAME );

    when( sql.getWhereClause() ).thenReturn( null );
    when( serviceTrans.getTransMeta().listParameters() ).thenReturn( new String[0] );

    RowListener clientRowListener = mock( RowListener.class );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( sql, service ).
        serviceTrans( serviceTrans ).
        sqlTransGenerator( sqlTransGenerator ).
        genTrans( genTrans ).
        build();

    // Start Execution
    executor.executeQuery( clientRowListener );

    InOrder genTransStartup = inOrder( genTrans, resultStep );
    InOrder serviceTransStartup = inOrder( serviceTrans, serviceStep );
    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> resultStepListener = ArgumentCaptor.forClass( StepListener.class );

    genTransStartup.verify( genTrans ).addRowProducer( INJECTOR_STEP_NAME, 0 );
    genTransStartup.verify( resultStep ).addStepListener( resultStepListener.capture() );
    genTransStartup.verify( resultStep ).addRowListener( clientRowListener );
    genTransStartup.verify( genTrans ).startThreads();

    serviceTransStartup.verify( serviceStep ).addRowListener( listenerArgumentCaptor.capture() );
    serviceTransStartup.verify( serviceTrans ).startThreads();

    // Verify linkage
    RowListener serviceRowListener = listenerArgumentCaptor.getValue();
    assertNotNull( serviceRowListener );

    RowProducer sqlTransRowProducer = genTrans.addRowProducer( INJECTOR_STEP_NAME, 0 );
    // Push row from service to sql Trans
    RowMeta rowMeta = mock( RowMeta.class );
    for ( int i = 0; i < 50; i++ ) {
      Object[] data = new Object[] { i };

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
  }

  @Test
  public void testQueryWithParams() throws Exception {
    String sql = "SELECT * FROM FOO WHERE PARAMETER('foo') = 'bar' AND PARAMETER('baz') = 'bop'";
    DataServiceMeta service = createDataServiceMeta();
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();

    final SQL theSql = new SQL( sql );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( theSql, service ).
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
    String sql = "SELECT * FROM table WHERE column1 IS NOT NULL AND column2 IS NULL";

    DataServiceMeta service = createDataServiceMeta();
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SqlTransGenerator sqlTransGenerator = mockSqlTransGenerator();

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), service ).
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
  public void testWithLazyConversion () throws Exception {
    RowMeta rowMeta = new RowMeta();
    ValueMeta vm = new ValueMeta( "aBinaryStoredString", ValueMeta.TYPE_STRING, ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
    vm.setStorageMetadata( new ValueMetaString() );
    rowMeta.addValueMeta( vm );

    String query = "SELECT * FROM " + SERVICE_NAME + " WHERE aBinaryStoredString = 'value'";

    DataServiceMeta service = createDataServiceMeta();
    TransMeta transMeta = mockTransMeta();
    when( transMeta.getStepFields( SERVICE_STEP_NAME ) ).thenReturn( rowMeta );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), service ).
        serviceTrans( new Trans(transMeta) ).
        prepareExecution( false ).
        build();

    executor.getSql().getWhereCondition().getCondition().evaluate( rowMeta, new Object[] {"value".getBytes()} );
  }

  private SqlTransGenerator mockSqlTransGenerator() {
    SqlTransGenerator sqlTransGenerator = mock( SqlTransGenerator.class );
    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP_NAME );
    when( sqlTransGenerator.getResultStepName() ).thenReturn( RESULT_STEP_NAME );
    return sqlTransGenerator;
  }

  private ArrayList<DataServiceMeta> createServicesList( DataServiceMeta service ) {
    ArrayList<DataServiceMeta> list = Lists.newArrayListWithCapacity( 20 );
    for ( int i = 0; i < 19; i++ ) {
      DataServiceMeta falseMeta = mock( DataServiceMeta.class );
      when( falseMeta.getName() ).thenReturn( UUID.randomUUID().toString() );
      list.add( falseMeta );
    }
    list.add( service );
    return list;
  }

  private TransMeta mockTransMeta() throws KettleStepException {
    TransMeta trans = mock( TransMeta.class, RETURNS_DEEP_STUBS );
    when( trans.listVariables() ).thenReturn( new String[0] );
    when( trans.listParameters() ).thenReturn( new String[0] );
    Answer<TransMeta> clone = new Answer<TransMeta>() {
      private TransMeta clone = null;

      @Override public TransMeta answer( InvocationOnMock invocation ) throws Throwable {
        if ( clone == null ) {
          clone = mockTransMeta();
        }
        return clone;
      }
    };
    when( trans.realClone( anyBoolean() ) ).thenAnswer( clone );
    when( trans.clone() ).thenAnswer( clone );
    return trans;
  }

  private DataServiceMeta createDataServiceMeta() {
    DataServiceMeta service = new DataServiceMeta();
    service.setName( SERVICE_NAME );
    service.setStepname( SERVICE_STEP_NAME );
    return service;
  }

  private Repository mockRepository( ObjectId transId, TransMeta trans ) throws KettleException {
    Repository repository = mock( Repository.class );
    when( repository.loadTransformation( eq( transId ), anyString() ) ).thenReturn( trans );
    return repository;
  }
}
