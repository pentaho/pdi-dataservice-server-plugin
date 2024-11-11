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

package org.pentaho.di.trans.dataservice.execution;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.caching.api.PentahoCacheSystemConfiguration;
import org.pentaho.caching.impl.PentahoCacheManagerImpl;
import org.pentaho.caching.ri.HeapCacheProvidingService;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.UIFactory;
import org.pentaho.di.trans.steps.delay.DelayMeta;

import java.util.HashMap;
import java.util.concurrent.Executors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

public class DataServiceRealTest {
  @BeforeClass
  public static void setUp() throws Exception {
    KettleClientEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );

    StepPluginType.getInstance().handlePluginAnnotation(
      DelayMeta.class,
      DelayMeta.class.getAnnotation( org.pentaho.di.core.annotations.Step.class ),
      emptyList(), false, null );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
  }

  @Test
  public void testServiceTransKeepsRunningWhenDataServiceIsNotUserDefined() throws Exception {
    TransMeta transMeta = new TransMeta( getClass().getResource( "/GenerateOneMillion.ktr" ).getPath() );
    DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "table" );
    dataServiceMeta.setStepname( "Delay Row" );
    dataServiceMeta.setUserDefined( false );
    PentahoCacheSystemConfiguration systemConfiguration = new PentahoCacheSystemConfiguration();
    systemConfiguration.setData( new HashMap<>() );
    PentahoCacheManagerImpl pentahoCacheManager =
      new PentahoCacheManagerImpl( systemConfiguration, new HeapCacheProvidingService() );
    ServiceCacheFactory cacheFactory =
      new ServiceCacheFactory( pentahoCacheManager, Executors.newSingleThreadExecutor() );
    DataServiceContext dataServiceContext =
      new DataServiceContext( singletonList( cacheFactory ), emptyList(), pentahoCacheManager, new UIFactory(), new LogChannel( "" ) );

    SQL countSql = new SQL( "select count(*) from table" );
    DataServiceExecutor countExecutor =
      new DataServiceExecutor.Builder( countSql, dataServiceMeta, dataServiceContext ).build();
    countExecutor.executeQuery();
    SQL limitSql = new SQL( "select field1,field2 from table limit 5" );
    DataServiceExecutor limitExecutor =
      new DataServiceExecutor.Builder( limitSql, dataServiceMeta, dataServiceContext ).build();
    limitExecutor.executeQuery();
    limitExecutor.waitUntilFinished();
    assertTrue( countExecutor.getGenTrans().isRunning() );
    assertTrue( countExecutor.getServiceTrans().isRunning() );
    countExecutor.getServiceTrans().stopAll();
  }
}
