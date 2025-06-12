/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.caching.api.Constants;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.UIFactory;
import org.pentaho.di.trans.step.StepMeta;

import javax.cache.Cache;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public abstract class BaseTest {
  public static final String DATA_SERVICE_NAME = "DataService";
  public static final String STREAMING_DATA_SERVICE_NAME = "StreamingDataService";
  public static final String DATA_SERVICE_NAME2 = "DataService2";
  public static final String DATA_SERVICE_STEP = "Data Service Step";
  protected TransMeta transMeta;
  protected TransMeta streamingTransMeta;
  protected DataServiceMeta dataService;
  protected DataServiceMeta streamingDataService;

  protected DataServiceContext context;
  protected List<PushDownFactory> pushDownFactories;
  protected List<AutoOptimizationService> autoOptimizationServices;

  @Mock protected DataServiceMetaStoreUtil metaStoreUtil;
  @Mock protected PentahoCacheManager cacheManager;
  @Mock protected UIFactory uiFactory;
  @Mock protected LogChannelInterface logChannel;
  @Mock protected Cache<Integer, String> cache;
  @Mock protected DataServiceClient client;

  protected DataServiceDelegate delegate;
  @Mock protected Function<Exception, Void> exceptionHandler;

  protected DataServiceMeta createDataService( String dataServiceName, TransMeta transMeta ) {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( dataServiceName );
    dataService.setStepname( DATA_SERVICE_STEP );
    return dataService;
  }

  protected DataServiceMeta createStreamingDataService( String dataServiceName, TransMeta transMeta ) {
    DataServiceMeta dataService = new DataServiceMeta( transMeta, true );
    dataService.setName( dataServiceName );
    dataService.setStepname( DATA_SERVICE_STEP );
    return dataService;
  }

  protected TransMeta createTransMeta( String dataServiceTrans ) {
    try {
      KettleClientEnvironment.init();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
    TransMeta transMeta = spy( new TransMeta() );
    transMeta.setName( dataServiceTrans );
    transMeta.setObjectId( new StringObjectId( UUID.randomUUID().toString() ) );
    try {
      lenient().doNothing().when( transMeta ).activateParameters();
      lenient().doAnswer( RETURNS_DEEP_STUBS ).when( transMeta ).getStepFields( any( StepMeta.class ) );
      lenient().doAnswer( RETURNS_DEEP_STUBS ).when( transMeta ).getStepFields( anyString() );
    } catch ( KettleException e ) {
      Throwables.propagate( e );
    }
    return transMeta;
  }

  @Before
  public void setUpBase() throws Exception {
    transMeta = createTransMeta( DATA_SERVICE_NAME );
    streamingTransMeta = createTransMeta( STREAMING_DATA_SERVICE_NAME );

    StepMeta stepMeta = mock( StepMeta.class );
    lenient().when( stepMeta.getName() ).thenReturn( DATA_SERVICE_STEP );
    transMeta.addStep( stepMeta );
    transMeta.clearChanged();

    StepMeta streamingStepMeta = mock( StepMeta.class );
    lenient().when( streamingStepMeta.getName() ).thenReturn( STREAMING_DATA_SERVICE_NAME );
    streamingTransMeta.addStep( streamingStepMeta );
    streamingTransMeta.clearChanged();

    dataService = createDataService( DATA_SERVICE_NAME, transMeta );
    streamingDataService = createStreamingDataService( STREAMING_DATA_SERVICE_NAME, streamingTransMeta );

    PentahoCacheTemplateConfiguration template = mock( PentahoCacheTemplateConfiguration.class );
    lenient().when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( Constants.DEFAULT_TEMPLATE, template ) );
    lenient().when( template.createCache( anyString(), eq( Integer.class ), eq( String.class ) ) ).thenReturn( cache );

    context = new DataServiceContext(
      pushDownFactories = Lists.newArrayList(),
      autoOptimizationServices = Lists.newArrayList(),
      cacheManager,
      metaStoreUtil, uiFactory,
      logChannel
    );

    lenient().when( metaStoreUtil.getStepCache() ).thenReturn( cache );
    lenient().when( metaStoreUtil.getLogChannel() ).thenReturn( logChannel );
    lenient().when( client.getLogChannel() ).thenReturn( logChannel );
  }

  protected Matcher<SQL> sql( String sqlString ) {
    return hasProperty( "sqlString", equalTo( sqlString ) );
  }

}
