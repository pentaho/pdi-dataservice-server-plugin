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

import org.pentaho.di.trans.dataservice.cache.DataServiceMetaCache;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.Collections;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaStoreUtilTest {

  public static final String DATA_SERVICE_NAME = "DataServiceNameForLoad";
  public static final String DATA_SERVICE_STEP = "DataServiceStepNameForLoad";
  private TransMeta transMeta;
  private IMetaStore metaStore;
  private DataServiceMeta dataService;
  private MetaStoreFactory<DataServiceMeta> metaStoreFactory;

  @Mock
  private DataServiceMetaCache cache;

  @Mock
  private DataServiceMetaStoreUtil metaStoreUtil;

  @BeforeClass
  public static void init() throws KettleException {
    KettleEnvironment.init();
  }

  @Before
  public void setUp() throws KettleException, MetaStoreException {
    LogChannel.GENERAL = mock( LogChannelInterface.class );
    doNothing().when( LogChannel.GENERAL ).logBasic( anyString() );

    transMeta = new TransMeta();
    transMeta.setFilename( "/path/to/transformation.ktr" );

    metaStore = new MemoryMetaStore();
    dataService = new DataServiceMeta();
    dataService.setName( DATA_SERVICE_NAME );
    dataService.setStepname( DATA_SERVICE_STEP );
    dataService.setTransFilename( "/path/to/transformation.ktr" );

    doReturn( null ).when( cache ).get( any( TransMeta.class ), anyString() );

    metaStoreUtil = new DataServiceMetaStoreUtil( Collections.<PushDownFactory>singletonList( new PushDownFactory() {
      @Override public String getName() {
        return "Mock Optimization";
      }

      @Override public Class<? extends PushDownType> getType() {
        return createPushDown().getClass();
      }

      @Override public PushDownType createPushDown() {
        return mock( PushDownType.class );
      }

      @Override public PushDownOptTypeForm createPushDownOptTypeForm() {
        return mock( PushDownOptTypeForm.class );
      }
    } ), cache
    );
    metaStoreFactory = metaStoreUtil.getMetaStoreFactory( metaStore );
  }

  /**
   * Test cases for data service description loading
   *
   * @throws MetaStoreException
   */
  @Test
  public void testFromTransMeta() throws MetaStoreException {
    metaStoreFactory.saveElement( dataService );

    DataServiceMeta dataServiceMeta =
      metaStoreUtil.fromTransMeta( transMeta, metaStore, DATA_SERVICE_STEP );
    assertThat( dataServiceMeta, notNullValue() );

    assertEquals( DATA_SERVICE_NAME, dataServiceMeta.getName() );
    assertEquals( DATA_SERVICE_STEP, dataServiceMeta.getStepname() );
  }

  /**
   * Test cases for data service description saving
   *
   * @throws MetaStoreException
   */
  @Test
  public void testToTransMeta() throws MetaStoreException {
    metaStoreUtil.toTransMeta( transMeta, metaStore, dataService );
    assertEquals( DATA_SERVICE_STEP, metaStoreFactory.loadElement( DATA_SERVICE_NAME ).getStepname() );
  }

}
