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

package com.pentaho.di.trans.dataservice;

import com.google.common.collect.Lists;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransListener;
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;

public class DataServiceExecutorTest {

  public static final String SERVICE_NAME = "serviceName";
  public static final String SERVICE_STEP_NAME = "Service Step";
  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";

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
    assertEquals( trans, executor.getServiceTransMeta() );
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
    InOrder serviceTransStartup = inOrder( serviceTrans, serviceTrans.findRunThread( SERVICE_STEP_NAME ) );
    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> resultStepListener = ArgumentCaptor.forClass( StepListener.class );

    genTransStartup.verify( genTrans ).addRowProducer( INJECTOR_STEP_NAME, 0 );
    genTransStartup.verify( resultStep ).addStepListener( resultStepListener.capture() );
    genTransStartup.verify( resultStep ).addRowListener( clientRowListener );
    genTransStartup.verify( genTrans ).startThreads();

    serviceTransStartup.verify( serviceTrans.findRunThread( SERVICE_STEP_NAME ) ).addRowListener( listenerArgumentCaptor.capture() );
    serviceTransStartup.verify( serviceTrans ).startThreads();

    // Verify linkage
    RowListener serviceRowListener = listenerArgumentCaptor.getValue();
    assertNotNull( serviceRowListener );

    RowProducer sqlTransRowProducer = genTrans.addRowProducer( INJECTOR_STEP_NAME, 0 );
    // Push row from service to sql Trans
    for ( int i = 0; i < 50; i++ ) {
      RowMeta rowMeta =  mock( RowMeta.class );
      Object[] data = new Object[0];

      serviceRowListener.rowWrittenEvent( rowMeta, data );
      verify( sqlTransRowProducer ).putRow( same( rowMeta ), same( data ) );
    }

    doReturn( true ).when( serviceTrans ).isRunning();
    resultStepListener.getValue().stepFinished( genTrans, resultStep.getStepMeta(), resultStep );
    verify( serviceTrans ).stopAll();

    // Verify Service Trans finished
    ArgumentCaptor<TransListener> serviceTransListener = ArgumentCaptor.forClass( TransListener.class );
    verify( serviceTrans ).addTransListener( serviceTransListener.capture() );
    serviceTransListener.getValue().transFinished( serviceTrans );
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
    RowMetaInterface serviceStep = mock( RowMetaInterface.class, RETURNS_DEEP_STUBS );
    when( trans.getStepFields( SERVICE_STEP_NAME ) ).thenReturn( serviceStep );
    when( trans.listVariables() ).thenReturn( new String[0] );
    when( trans.listParameters() ).thenReturn( new String[0] );
    return trans;
  }

  private DataServiceMeta createDataServiceMeta() {
    DataServiceMeta service = new DataServiceMeta(  );
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
