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
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.UIFactory;
import org.pentaho.di.trans.step.StepMeta;

import javax.cache.Cache;
import java.util.List;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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
  protected DataServiceDelegate delegate;

  protected DataServiceMeta createDataService( String dataServiceName, TransMeta transMeta ) {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( dataServiceName );
    dataService.setStepname( DATA_SERVICE_STEP );
    return dataService;
  }

  protected TransMeta createTransMeta( String dataServiceTrans ) {
    TransMeta transMeta = new TransMeta();
    transMeta.setName( dataServiceTrans );
    transMeta.setObjectId( new StringObjectId( UUID.randomUUID().toString() ) );
    return transMeta;
  }

  @Before
  public void setUpBase() throws KettleException {
    transMeta = createTransMeta( "dataServiceTrans" );

    StepMeta stepMeta = mock( StepMeta.class );
    when( stepMeta.getName() ).thenReturn( DATA_SERVICE_STEP );
    transMeta.addStep( stepMeta );
    transMeta.clearChanged();

    dataService = createDataService( DATA_SERVICE_NAME, transMeta );

    PentahoCacheTemplateConfiguration template = mock( PentahoCacheTemplateConfiguration.class );
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( Constants.DEFAULT_TEMPLATE, template ) );
    when( template.createCache( anyString(), eq( Integer.class ), eq( String.class ) ) ).thenReturn( cache );

    context = new DataServiceContext(
      pushDownFactories = Lists.newArrayList(),
      autoOptimizationServices = Lists.newArrayList(),
      cacheManager, uiFactory, logChannel
    ) {
      @Override public DataServiceMetaStoreUtil getMetaStoreUtil() {
        return metaStoreUtil;
      }
    };

    when( metaStoreUtil.getContext() ).thenReturn( context );
    when( metaStoreUtil.getStepCache() ).thenReturn( cache );
    when( metaStoreUtil.getLogChannel() ).thenReturn( logChannel );
  }

}
