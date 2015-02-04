package com.pentaho.di.trans.dataservice;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

public class DataServiceMetaStoreUtilTest {

  public static final String DATA_SERVICE_NAME = "DataServiceNameForLoad";
  public static final String DATA_SERVICE_STEP = "DataServiceStepNameForLoad";
  private TransMeta transMeta;
  private IMetaStore metaStore;
  private DataServiceMeta dataService;
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

    metaStoreFactory = DataServiceMeta.getMetaStoreFactory( metaStore, PentahoDefaults.NAMESPACE );
  }

  /**
   * Test cases for data service description loading
   * 
   * @throws MetaStoreException
   */
  @Test
  public void testFromTransMeta() throws MetaStoreException {
    metaStoreFactory.saveElement( dataService );
    DataServiceMeta dataServiceMeta = DataServiceMetaStoreUtil.fromTransMeta( transMeta, metaStore );
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
    DataServiceMetaStoreUtil.toTransMeta( transMeta, metaStore, dataService, true );
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
