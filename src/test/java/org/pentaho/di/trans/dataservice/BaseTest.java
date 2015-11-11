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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.caching.api.Constants;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public abstract class BaseTest {
  public static final String DATA_SERVICE_NAME = "DataService";
  public static final String DATA_SERVICE_STEP = "Data Service Step";
  protected TransMeta transMeta;
  protected DataServiceMeta dataService;

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

  protected TransMeta createTransMeta( String dataServiceTrans ) {
    TransMeta transMeta = spy( new TransMeta() );
    transMeta.setName( dataServiceTrans );
    transMeta.setObjectId( new StringObjectId( UUID.randomUUID().toString() ) );
    try {
      doNothing().when( transMeta ).activateParameters();
      doAnswer( RETURNS_DEEP_STUBS ).when( transMeta ).getStepFields( any( StepMeta.class ) );
    } catch ( KettleException e ) {
      Throwables.propagate( e );
    }
    return transMeta;
  }

  @Before
  public void setUpBase() throws Exception {
    transMeta = createTransMeta( "dataServiceTrans" );

    StepMeta stepMeta = mock( StepMeta.class );
    when( stepMeta.getName() ).thenReturn( DATA_SERVICE_STEP );
    transMeta.addStep( stepMeta );
    transMeta.clearChanged();

    dataService = createDataService( DATA_SERVICE_NAME, transMeta );

    PentahoCacheTemplateConfiguration template = mock( PentahoCacheTemplateConfiguration.class );
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( Constants.DEFAULT_TEMPLATE, template ) );
    when( template.createCache( anyString(), eq( Integer.class ), eq( String.class ) ) ).thenReturn( cache );

    context = mock( DataServiceContext.class );
    when( context.getPushDownFactories() ).thenReturn( pushDownFactories = Lists.newArrayList() );
    when( context.getAutoOptimizationServices() ).thenReturn( autoOptimizationServices = Lists.newArrayList() );
    when( context.getCacheManager() ).thenReturn( cacheManager );
    when( context.getUIFactory() ).thenReturn( uiFactory );
    when( context.getLogChannel() ).thenReturn( logChannel );
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    when( context.createBuilder( any( SQL.class ), same( dataService ) ) ).then( RETURNS_DEEP_STUBS );

    when( metaStoreUtil.getContext() ).thenReturn( context );
    when( metaStoreUtil.getStepCache() ).thenReturn( cache );
    when( metaStoreUtil.getLogChannel() ).thenReturn( logChannel );
  }

}
