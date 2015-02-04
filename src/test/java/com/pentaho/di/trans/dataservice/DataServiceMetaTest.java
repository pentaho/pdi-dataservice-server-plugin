/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
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

import org.junit.Test;
import org.pentaho.di.core.sql.ServiceCacheMethod;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

import static org.junit.Assert.assertEquals;

public class DataServiceMetaTest {

  @Test
  public void testDataServiceMetaSerialization() throws MetaStoreException {
    IMetaStore metaStore = new MemoryMetaStore();
    DataServiceMeta[] dataServiceMetas = new DataServiceMeta[] {
      makeTestDSM( "name", "stepName", "/my/transFilename.ktr", "transRepPath", null, ServiceCacheMethod.None, 5 ),
      makeTestDSM( "name2", "stepName 2", "/foo/bar/baz.ktr", "transRepPath", "otherOid", ServiceCacheMethod.None, 15 ),
      makeTestDSM( "name 3", "stepName3", null, "transRepPath", "blahOid", ServiceCacheMethod.LocalMemory, 0 ),
    };
    MetaStoreFactory<DataServiceMeta> factory = new MetaStoreFactory<DataServiceMeta>(
      DataServiceMeta.class, metaStore, PentahoDefaults.NAMESPACE );

    for ( DataServiceMeta meta : dataServiceMetas ) {
      factory.saveElement( meta );
    }
    for ( DataServiceMeta meta : dataServiceMetas ) {
      DataServiceMeta returnedDSM = factory.loadElement( meta.getName() );
      assertEquals( meta.getStepname(), returnedDSM.getStepname() );
      assertEquals( meta.getTransFilename(), returnedDSM.getTransFilename() );
      assertEquals( meta.getTransRepositoryPath(), returnedDSM.getTransRepositoryPath() );
      assertEquals( meta.getTransObjectId(), returnedDSM.getTransObjectId() );
      assertEquals( meta.getCacheMethod(), returnedDSM.getCacheMethod() );
      assertEquals( meta.getCacheMaxAgeMinutes(), returnedDSM.getCacheMaxAgeMinutes() );
    }
  }

  private DataServiceMeta makeTestDSM( String name, String stepName, String transFilename,
                                       String transRepPath, String transOid,
                                       ServiceCacheMethod cacheMethod, int cacheAgeMinutes ) {
    DataServiceMeta dsm = new DataServiceMeta();
    dsm.setName( name );
    dsm.setStepname( stepName );
    dsm.setTransFilename( transFilename );
    dsm.setTransRepositoryPath( transRepPath );
    dsm.setTransObjectId( transOid );
    dsm.setCacheMethod( cacheMethod );
    dsm.setCacheMaxAgeMinutes( cacheAgeMinutes );
    return dsm;
  }
}
