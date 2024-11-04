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


package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.util.List;

/**
 * Created by bmorrise on 8/30/16.
 */
public interface DataServiceResolver {
  DataServiceMeta getDataService( String dataServiceName );
  List<DataServiceMeta> getDataServices( Function<Exception, Void> logger );
  List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger );
  List<String> getDataServiceNames();
  List<String> getDataServiceNames( String dataServiceName );
  DataServiceExecutor.Builder createBuilder( SQL sql ) throws KettleException;
}
