/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.sql.SqlTransMeta;
import org.pentaho.di.trans.step.RowListener;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceExecutorTest {

  public static final String SERVICE_NAME = "serviceName";
  public static final String SERVICE_STEP_NAME = "Service Step";
  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";

  @Test
  public void testCreateExecutorByObjectId() throws Exception {
    String query = "SELECT field1, field2 FROM " + SERVICE_NAME;

    DataServiceMeta service = mockDataServiceMeta();
    ObjectId transId = new StringObjectId( UUID.randomUUID().toString() );
    when( service.getTransObjectId() ).thenReturn( transId.getId() );

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
    DataServiceMeta serviceMeta = mockDataServiceMeta();
    Trans serviceTrans = mock( Trans.class );
    TransMeta transMeta = mockTransMeta();
    when( serviceTrans.getTransMeta() ).thenReturn( transMeta );
    Trans genTrans = mock( Trans.class );

    new DataServiceExecutor.Builder( new SQL( "SELECT foo FROM bar" ), serviceMeta ).
      serviceTrans( serviceTrans ).
      genTrans( genTrans ).
      prepareExecution( false ).
      logLevel( LogLevel.DETAILED ).
      build();

    verify( serviceTrans ).setLogLevel( LogLevel.DETAILED );
    verify( genTrans ).setLogLevel( LogLevel.DETAILED );
  }

  @Test
  public void testConditionResolution() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "aString", ValueMeta.TYPE_STRING ) );
    rowMeta.addValueMeta( new ValueMeta( "anInt", ValueMeta.TYPE_INTEGER ) );
    rowMeta.addValueMeta( new ValueMeta( "aDate", ValueMeta.TYPE_DATE ) );

    String query = "SELECT * FROM " + SERVICE_NAME + " WHERE anInt = 2 AND aDate IN ('2014-12-05','2008-01-01')";

    DataServiceMeta service = mockDataServiceMeta();
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
      DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( query ), services ).
          lookupServiceTrans( mock( Repository.class ) ).
          prepareExecution( false ).
          build();

      // Verify execution prep
      assertNull( executor.getServiceTrans() );
      assertNull( executor.getServiceTransMeta() );
      assertNotNull( executor.getGenTransMeta() );
      assertNotNull( executor.getGenTrans() );
      assertTrue( executor.isDual() );

      Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
      SqlTransMeta sqlTransMeta = mockSqlMetaTrans();

      executor = new DataServiceExecutor.Builder( new SQL( query ), services ).
        lookupServiceTrans( mock( Repository.class ) ).
        sqlTransGenerator( sqlTransMeta ).
        genTrans( genTrans ).
        prepareExecution( false ).
        build();
      RowListener clientRowListener = mock( RowListener.class );

      // Start Execution
      executor.executeQuery( clientRowListener );

      InOrder startup = inOrder( genTrans, genTrans.findRunThread( RESULT_STEP_NAME ) );

      startup.verify( genTrans.findRunThread( RESULT_STEP_NAME ) ).addRowListener( clientRowListener );
      startup.verify( genTrans ).startThreads();
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {

    DataServiceMeta service = mockDataServiceMeta();
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SQL sql = mock( SQL.class );
    when( sql.getServiceName() ).thenReturn( "test_service" );
    SqlTransMeta sqlTransMeta = mockSqlMetaTrans();

    when( sql.getWhereClause() ).thenReturn( null );
    when( serviceTrans.getTransMeta().listParameters() ).thenReturn( new String[0] );

    RowListener clientRowListener = mock( RowListener.class );

    DataServiceExecutor executor = new DataServiceExecutor.Builder( sql, service ).
        serviceTrans( serviceTrans ).
        sqlTransGenerator( sqlTransMeta ).
        genTrans( genTrans ).
        build();

    // Start Execution
    executor.executeQuery( clientRowListener );

    InOrder genTransStartup = inOrder( genTrans, genTrans.findRunThread( RESULT_STEP_NAME ) );
    InOrder serviceTransStartup = inOrder( serviceTrans, serviceTrans.findRunThread( SERVICE_STEP_NAME ) );
    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );

    genTransStartup.verify( genTrans ).addRowProducer( INJECTOR_STEP_NAME, 0 );
    genTransStartup.verify( genTrans.findRunThread( RESULT_STEP_NAME ) ).addRowListener( clientRowListener );
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

    // Verify Service Trans finished
    ArgumentCaptor<TransListener> serviceTransListener = ArgumentCaptor.forClass( TransListener.class );
    verify( serviceTrans ).addTransListener( serviceTransListener.capture() );
    serviceTransListener.getValue().transFinished( serviceTrans );
    verify( sqlTransRowProducer ).finished();
  }

  @Test
  public void testQueryWithParams() throws Exception {
    String sql = "SELECT * FROM FOO WHERE PARAMETER('foo') = 'bar' AND PARAMETER('baz') = 'bop'";
    DataServiceMeta service = mockDataServiceMeta();
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    SqlTransMeta sqlTransMeta = mockSqlMetaTrans();

    DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sql ), service ).
      serviceTrans( serviceTrans ).
      sqlTransGenerator( sqlTransMeta ).
      genTrans( genTrans ).
      build();
    Map<String, String> expectedParams = new HashMap<String, String>();
    expectedParams.put( "baz", "bop" );
    expectedParams.put( "foo", "bar" );

    assertEquals( expectedParams, executor.getParameters() );
  }

  private SqlTransMeta mockSqlMetaTrans() {
    SqlTransMeta sqlTransMeta = mock( SqlTransMeta.class );
    when( sqlTransMeta.getInjectorStepName() ).thenReturn( INJECTOR_STEP_NAME );
    when( sqlTransMeta.getResultStepName() ).thenReturn( RESULT_STEP_NAME );
    return sqlTransMeta;
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

  private DataServiceMeta mockDataServiceMeta() {
    DataServiceMeta service = mock( DataServiceMeta.class );
    when( service.getName() ).thenReturn( SERVICE_NAME );
    when( service.getStepname() ).thenReturn( SERVICE_STEP_NAME );
    return service;
  }

  private Repository mockRepository( ObjectId transId, TransMeta trans ) throws KettleException {
    Repository repository = mock( Repository.class );
    when( repository.loadTransformation( eq( transId ), anyString() ) ).thenReturn( trans );
    return repository;
  }
}
