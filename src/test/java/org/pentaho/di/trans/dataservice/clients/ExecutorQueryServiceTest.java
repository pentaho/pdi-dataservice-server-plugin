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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.util.HashMap;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutorQueryServiceTest {
  @Test
  public void testQueryBuildsWithMetastore() throws Exception {
    final DataServiceFactory factory = mock( DataServiceFactory.class );
    final DataServiceContext context = mock( DataServiceContext.class );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );
    when( context.getMetaStoreUtil() ).thenReturn( factory );
    DataServiceExecutor.Builder builder = mock( DataServiceExecutor.Builder.class );

    final IMetaStore metastore = mock( IMetaStore.class );
    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );

    SQL sql = new SQL( "select field from table" );
    HashMap<String, String> parameters = new HashMap<>();
    int rowLimit = 5432;

    ExecutorQueryService executorQueryService = new ExecutorQueryService( dataServiceResolver, metastoreLocator );
    when( dataServiceResolver.createBuilder( argThat( matchesSql( sql ) ) ) ).thenReturn( builder );
    when( factory.getMetaStore() ).thenReturn( metastore );

    when( builder.rowLimit( rowLimit ) ).thenReturn( builder );
    when( builder.parameters( parameters ) ).thenReturn( builder );
    when( builder.metastore( metastore ) ).thenReturn( builder );

    executorQueryService.prepareQuery( sql.getSqlString(), rowLimit, parameters );

    verify( builder ).rowLimit( rowLimit );
    verify( builder ).parameters( parameters );
    verify( builder ).metastore( metastore );
  }

  private Matcher<SQL> matchesSql( final SQL sql ) {
    return new BaseMatcher<SQL>() {
      @Override public boolean matches( final Object o ) {
        return ( (SQL) o ).getSqlString().equals( sql.getSqlString() );
      }

      @Override public void describeTo( final Description description ) {

      }
    };
  }
}
