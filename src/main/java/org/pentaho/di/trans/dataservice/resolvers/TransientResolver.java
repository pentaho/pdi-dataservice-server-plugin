/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryLocator;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Created by bmorrise on 8/30/16.
 */
public class TransientResolver implements DataServiceResolver {

  public static final String DELIMITER = ":";
  public static final String PREFIX = "transient:";
  private KettleRepositoryLocator repositoryLocator;
  private DataServiceContext context;
  private ServiceCacheFactory cacheFactory;

  public TransientResolver( KettleRepositoryLocator repositoryLocator, DataServiceContext context,
                            ServiceCacheFactory cacheFactory ) {
    this.repositoryLocator = repositoryLocator;
    this.context = context;
    this.cacheFactory = cacheFactory;
  }

  @Override
  public DataServiceMeta getDataService( String dataServiceName ) {
    if ( !isTransient( dataServiceName ) ) {
      return null;
    }
    return createDataServiceMeta( dataServiceName );
  }

  @Override public List<String> getDataServiceNames( String dataServiceName ) {
    List<String> dataServiceNames = new ArrayList<>();
    if ( isTransient( dataServiceName ) ) {
      dataServiceNames.add( dataServiceName );
    }
    return dataServiceNames;
  }

  @Override public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    if ( isTransient( dataServiceName ) ) {
      dataServiceMetas.add( createDataServiceMeta( dataServiceName ) );
    }
    return dataServiceMetas;
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) {
    DataServiceMeta dataServiceMeta = getDataService( sql.getServiceName() );
    if ( dataServiceMeta != null ) {
      return new DataServiceExecutor.Builder( sql, dataServiceMeta, context );
    }
    return null;
  }


  private DataServiceMeta createDataServiceMeta( String dataServiceName ) {
    String[] parts = splitTransient( dataServiceName );
    try {
      String fileAndPath = decode( parts[ 0 ].trim() );
      TransMeta transMeta;
      Repository repository = repositoryLocator != null ? repositoryLocator.getRepository() : null;
      if ( repository == null ) {
        transMeta = new TransMeta( fileAndPath );
      } else {
        transMeta = loadFromRepository( repository, fileAndPath );
      }
      String stepName = decode( String.valueOf( parts[ 1 ].trim() ) );
      DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
      dataServiceMeta.setStepname( stepName );
      dataServiceMeta.setName( dataServiceName );
      PushDownOptimizationMeta pushDownMeta = new PushDownOptimizationMeta();
      pushDownMeta.setStepName( stepName );
      pushDownMeta.setType( cacheFactory.createPushDown() );
      dataServiceMeta.setPushDownOptimizationMeta( Collections.singletonList( pushDownMeta ) );

      return dataServiceMeta;
    } catch ( Exception e ) {
      return null;
    }
  }

  private TransMeta loadFromRepository( Repository repository, String filePath ) throws KettleException {
    String name = filePath.substring( filePath.lastIndexOf( "/" ) + 1, filePath.length() );
    String path = filePath.substring( 0, filePath.lastIndexOf( "/" ) );

    RepositoryDirectoryInterface root = repository.loadRepositoryDirectoryTree();
    RepositoryDirectoryInterface rd = root.findDirectory( path );
    if ( rd == null ) {
      rd = root; // root
    }
    return repository.loadTransformation( repository.getTransformationID( name, rd ), null );
  }

  public static boolean isTransient( String dataServiceName ) {
    return dataServiceName.startsWith( PREFIX );
  }

  public static String buildTransient( String filePath, String stepName ) {
    try {
      return PREFIX + encode( filePath ) + DELIMITER + encode( stepName );
    } catch ( UnsupportedEncodingException e ) {
      return null;
    }
  }

  private static String encode( String value ) throws UnsupportedEncodingException {
    return Base64.getEncoder().encodeToString( value.getBytes( "utf-8" ) );
  }

  private static String decode( String value ) throws UnsupportedEncodingException {
    return new String( Base64.getDecoder().decode( value ), "utf-8" );
  }

  public String[] splitTransient( String dataServiceName ) {
    return dataServiceName.replace( PREFIX, "" ).split( DELIMITER );
  }

  @Override public List<String> getDataServiceNames() {
    return new ArrayList<>();
  }

  @Override public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    return new ArrayList<>();
  }
}
