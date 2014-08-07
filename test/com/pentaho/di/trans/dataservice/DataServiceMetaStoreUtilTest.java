package com.pentaho.di.trans.dataservice;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.metastore.PurRepositoryMetaStore;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.xml.XmlMetaStoreAttribute;
import org.pentaho.metastore.stores.xml.XmlMetaStoreElement;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;

public class DataServiceMetaStoreUtilTest {

  private String dataServiceNameForLoad;
  private String dataServiceStepNameForLoad;
  private String dataServiceNameForSave;
  private String dataServiceStepNameForSave;
  private TransMeta transMeta;
  private IMetaStore metaStore;

  @Before
  public void setUp() throws KettleException, MetaStoreException {
    LogChannel.GENERAL = mock( LogChannelInterface.class );
    doNothing().when( LogChannel.GENERAL ).logBasic( anyString() );

    dataServiceNameForLoad = "DataServiceNameForLoad";
    dataServiceStepNameForLoad = "DataServiceStepNameForLoad";
    dataServiceNameForSave = "DataServiceNameForSave";
    dataServiceStepNameForSave = "DataServiceStepNameForSave";

    transMeta = spy( new TransMeta() );
    transMeta.setAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_NAME,
        dataServiceNameForLoad );
    transMeta.setAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE,
        DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME, dataServiceStepNameForLoad );

    IUnifiedRepository unifiedRepository = mock( IUnifiedRepository.class );
    RepositoryFile repositoryFile = mock( RepositoryFile.class );

    PurRepository repository = mock( PurRepository.class );
    when( repository.getPur() ).thenReturn( unifiedRepository );
    when( repository.getPur().getFile( anyString() ) ).thenReturn( repositoryFile );

    IMetaStoreAttribute attribute = new XmlMetaStoreAttribute();
    attribute.setId( DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME );
    attribute.setValue( dataServiceStepNameForLoad );
    IMetaStoreElementType type = mock( IMetaStoreElementType.class );
    IMetaStoreElement element = new XmlMetaStoreElement();
    element.setName( dataServiceNameForLoad );
    element.setElementType( type );
    element.addChild( attribute );

    metaStore = spy( new PurRepositoryMetaStore( repository ) );
    doReturn( type ).when( metaStore ).getElementTypeByName( anyString(), anyString() );
    doReturn( element ).when( metaStore ).getElementByName( anyString(), any( IMetaStoreElementType.class ),
        anyString() );
  }

  /**
   * Test cases for data service description loading
   * 
   * @throws MetaStoreException
   */
  @Test
  public void testFromTransMeta() throws MetaStoreException {
    DataServiceMeta dataServiceMeta = DataServiceMetaStoreUtil.fromTransMeta( transMeta, metaStore );
    assertEquals( dataServiceNameForLoad, dataServiceMeta.getName() );
    assertEquals( dataServiceStepNameForLoad, dataServiceMeta.getStepname() );
  }

  /**
   * Test cases for data service description saving
   * 
   * @throws MetaStoreException
   */
  @Test
  public void testToTransMeta() throws MetaStoreException {
    DataServiceMeta dataService = new DataServiceMeta();
    dataService.setName( dataServiceNameForSave );
    dataService.setStepname( dataServiceStepNameForSave );
    DataServiceMetaStoreUtil.toTransMeta( transMeta, metaStore, dataService, false );
    String savedServiceName =
        transMeta
            .getAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_NAME );
    String savedServiceStepName =
        transMeta.getAttribute( DataServiceMetaStoreUtil.GROUP_DATA_SERVICE,
            DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME );
    assertEquals( dataServiceNameForSave, savedServiceName );
    assertEquals( dataServiceStepNameForSave, savedServiceStepName );
  }

}
