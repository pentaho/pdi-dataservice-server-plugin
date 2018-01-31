/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public class DualQueryService implements Query.Service {
  public static final String DUMMY_TABLE_NAME = "dual";
  @VisibleForTesting protected static final byte[] DATA;

  static {
    // Prime query service with canned response
    try {
      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream( arrayOutputStream );

      DataServiceExecutor.writeMetadata( dos, DUMMY_TABLE_NAME, "", "", "", "" );

      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "DUMMY" ) );
      rowMeta.writeMeta( dos );

      Object[] row = new Object[] { "x" };
      rowMeta.writeData( dos, row );

      DATA = arrayOutputStream.toByteArray();
    } catch ( Exception e ) {
      // Should never happen, since we're writing to an byte array
      throw Throwables.propagate( e );
    }
  }

  public DualQueryService() {
  }

  @Override public Query prepareQuery( String sqlString, int maxRows, Map<String, String> parameters ) {
    SQL sql;
    try {
      sql = new SQL( sqlString );
    } catch ( KettleSQLException e ) {
      return null;
    }

    if ( sql.getServiceName() == null || sql.getServiceName().equals( DUMMY_TABLE_NAME ) ) {
      // Support for SELECT 1 and SELECT 1 FROM dual
      return new DualQuery();
    } else {
      return null;
    }
  }

  private class DualQuery implements Query {
    public DualQuery() {
    }

    @Override public void writeTo( OutputStream outputStream ) throws IOException {
      outputStream.write( DATA );
    }

    @Override public List<Trans> getTransList() {
      return ImmutableList.of();
    }
  }
}
