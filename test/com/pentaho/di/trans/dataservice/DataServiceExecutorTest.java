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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceExecutorTest extends DataServiceExecutor {

  public static final String SERVICE_NAME = "serviceName";
  public static final String SERVICE_STEP_NAME = "Service Step";
  public static final String INJECTOR_STEP_NAME = "Injector Step";
  public static final String RESULT_STEP_NAME = "Result Step";

  public DataServiceExecutorTest() {
    super();
  }

  /**
   * This test class extends DataServiceExecutor to override initialization behavior with
   * deep dependencies.  Trans.prepareExecution() makes mocking very difficult.  We sidestep
   * that by overriding prepareExec w/ a no-op.
   */
  public DataServiceExecutorTest( String query, List<DataServiceMeta> services,
                                  Map<String, String> parameters, Repository repository, int i )
    throws KettleException {
    super( query, services, parameters, repository, i );
  }

  @Test
  public void testCreateExecutorByObjectId() throws Exception {
    String query = "SELECT field1, field2 FROM " + SERVICE_NAME;

    DataServiceMeta service = mockDataServiceMeta();
    ObjectId transId = new StringObjectId( UUID.randomUUID().toString() );
    when( service.getTransObjectId() ).thenReturn( transId.getId() );

    TransMeta trans = mockTransMeta();

    Repository repository = mockRepository( transId, trans );

    Map<String, String> parameters = Collections.emptyMap();

    List<DataServiceMeta> services = createServicesList( service );
    DataServiceExecutor executor = new DataServiceExecutorTest( query, services, parameters, repository, 0 );

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
  public void testConditionResolution() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "aString", ValueMeta.TYPE_STRING ) );
    rowMeta.addValueMeta( new ValueMeta( "anInt", ValueMeta.TYPE_INTEGER ) );
    rowMeta.addValueMeta( new ValueMeta( "aDate", ValueMeta.TYPE_DATE ) );

    String query = "SELECT * FROM " + SERVICE_NAME + " WHERE anInt = 2 AND aDate IN ('2014-12-05','2008-01-01')";

    Map<String, String> parameters = Collections.emptyMap();
    DataServiceMeta service = mockDataServiceMeta();
    ObjectId transId = new StringObjectId( UUID.randomUUID().toString() );
    when( service.getTransObjectId() ).thenReturn( transId.getId() );
    TransMeta transMeta = mockTransMeta();
    when( transMeta.getStepFields( SERVICE_STEP_NAME ) ).thenReturn( rowMeta );
    Repository repository = mockRepository( transId, transMeta );

    DataServiceExecutor executor = new DataServiceExecutorTest( query, Arrays.asList( service ), parameters, repository, 0 );

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
    Map<String, String> parameters = Collections.emptyMap();
    Repository repository = mock( Repository.class );

    for ( String query : queries ) {
      DataServiceExecutor executor = new DataServiceExecutorTest( query, services, parameters, repository, 0 );

      // Verify execution prep
      assertNull( executor.getServiceTrans() );
      assertNull( executor.getServiceTransMeta() );
      assertNotNull( executor.getGenTransMeta() );
      assertNotNull( executor.getGenTrans() );
      assertTrue( executor.isDual() );

      Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
      executor.setGenTrans( genTrans );
      SqlTransMeta sqlTransMeta = mockSqlMetaTrans();
      executor.setSqlTransMeta( sqlTransMeta );

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
    DataServiceExecutor executor = new DataServiceExecutor();

    DataServiceMeta service = mockDataServiceMeta();
    executor.setService( service );
    Trans serviceTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    executor.setServiceTrans( serviceTrans );
    Trans genTrans = mock( Trans.class, RETURNS_DEEP_STUBS );
    executor.setGenTrans( genTrans );
    SQL sql = mock( SQL.class );
    executor.setSql( sql );
    SqlTransMeta sqlTransMeta = mockSqlMetaTrans();
    executor.setSqlTransMeta( sqlTransMeta );
    executor.setServiceName( "test_service" );

    when( sql.getWhereClause() ).thenReturn( null );
    when( serviceTrans.getTransMeta().listParameters() ).thenReturn( new String[0] );

    RowListener clientRowListener = mock( RowListener.class );


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

  @Override
  protected void prepareExecution() {
    // no-op, skip Trans.prepareExecution().
  }

}
