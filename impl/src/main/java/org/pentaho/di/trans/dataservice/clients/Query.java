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

package org.pentaho.di.trans.dataservice.clients;

import io.reactivex.Observer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public interface Query {

  void writeTo( OutputStream outputStream ) throws IOException;

  List<Trans> getTransList();

  default void pushTo( Observer<List<RowMetaAndData>> streamingWindowConsumer ) throws Exception {
    throw new UnsupportedOperationException();
  }

  interface Service {
    Query prepareQuery( String sql, int maxRows, Map<String, String> parameters ) throws KettleException;
    Query prepareQuery( String sql, IDataServiceClientService.StreamingMode windowMode,
                        long windowSize, long windowEvery, long windowLimit,
                        Map<String, String> parameters ) throws KettleException;
  }
}
