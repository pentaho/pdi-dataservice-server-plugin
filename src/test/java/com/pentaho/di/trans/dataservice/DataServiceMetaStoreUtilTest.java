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

import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaStoreUtilTest {

  public static final String DATA_SERVICE_NAME = "DataServiceNameForLoad";
  public static final String DATA_SERVICE_STEP = "DataServiceStepNameForLoad";
  private TransMeta transMeta;
  private IMetaStore metaStore;
  private DataServiceMeta dataService;

  @Mock private DataServiceMetaStoreUtil metaStoreUtil;
  private MetaStoreFactory<DataServiceMeta>
    metaStoreFactory;

  @Before
  public void setUp() throws KettleException, MetaStoreException {
    LogChannel.GENERAL = mock( LogChannelInterface.class );
    doNothing().when( LogChannel.GENERAL ).logBasic( anyString() );

    transMeta = new TransMeta();
    transMeta.setAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_NAME,
      DATA_SERVICE_NAME );
    transMeta.setAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE,
      DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME, DATA_SERVICE_STEP );

    metaStore = new MemoryMetaStore();
    dataService = new DataServiceMeta();
    dataService.setName( DATA_SERVICE_NAME );
    dataService.setStepname( DATA_SERVICE_STEP );

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
    } )
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
      metaStoreUtil.fromTransMeta( transMeta, metaStore );
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
    String savedServiceName =
      transMeta
        .getAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_NAME );
    String savedServiceStepName =
      transMeta.getAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE,
        DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME );
    assertEquals( DATA_SERVICE_NAME, savedServiceName );
    assertEquals( DATA_SERVICE_STEP, savedServiceStepName );
    assertEquals( DATA_SERVICE_STEP, metaStoreFactory.loadElement( DATA_SERVICE_NAME ).getStepname() );
  }

}
