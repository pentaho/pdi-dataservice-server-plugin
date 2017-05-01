/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryLocator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by bmorrise on 8/30/16.
 */
public class TransientResolver implements DataServiceResolver {

  public static final String DELIMITER = ":";
  public static final String PREFIX = "transient:";
  public static final String LOCAL = "local:";
  private KettleRepositoryLocator repositoryLocator;
  private DataServiceContext context;
  private ServiceCacheFactory cacheFactory;
  private LogLevel logLevel;
  private Supplier<Spoon> spoonSupplier;

  public TransientResolver( KettleRepositoryLocator repositoryLocator, DataServiceContext context,
                            ServiceCacheFactory cacheFactory, final LogLevel logLevel ) {
    this( repositoryLocator, context, cacheFactory, logLevel, Spoon::getInstance );
  }

  public TransientResolver( KettleRepositoryLocator repositoryLocator, DataServiceContext context,
                            ServiceCacheFactory cacheFactory, final LogLevel logLevel, Supplier<Spoon> spoonSupplier ) {
    this.repositoryLocator = repositoryLocator;
    this.context = context;
    this.cacheFactory = cacheFactory;
    this.logLevel = logLevel;
    this.spoonSupplier = spoonSupplier;
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

  @Override public List<DataServiceMeta> getDataServices( String dataServiceName,
                                                          com.google.common.base.Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    if ( isTransient( dataServiceName ) ) {
      dataServiceMetas.add( createDataServiceMeta( dataServiceName ) );
    }
    return dataServiceMetas;
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) {
    DataServiceMeta dataServiceMeta = getDataService( sql.getServiceName() );
    if ( dataServiceMeta != null ) {
      return new DataServiceExecutor.Builder( sql, dataServiceMeta, context ).logLevel( logLevel );
    }
    return null;
  }


  private DataServiceMeta createDataServiceMeta( String dataServiceName ) {
    final String fileAndPath, rowLimit;
    String stepName;
    boolean local = false;
    try {
      String[] parts = splitTransient( dataServiceName );
      fileAndPath = decode( parts[ 0 ].trim() );
      stepName = decode( parts[ 1 ].trim() );
      if ( stepName.startsWith( LOCAL ) ) {
        local = true;
        stepName = stepName.replace( LOCAL, "" );
      }
      rowLimit = parts.length >= 3 ? decode( parts[ 2 ].trim() ) : null;
    } catch ( Exception ignored ) {
      return null;
    }

    Optional<TransMeta> transMeta;
    if ( local && spoonSupplier.get() != null && spoonSupplier.get().getActiveTransformation() != null ) {
      transMeta = Optional.of( (TransMeta) spoonSupplier.get().getActiveTransformation().realClone( false ) );
    } else {
      // Try to locate the transformation, repository first
      transMeta = Stream.of( loadFromRepository(), TransMeta::new )
        .map( loader -> loader.tryLoad( fileAndPath ).orElse( null ) )
        .filter( Objects::nonNull )
        .findFirst();
    }

    // Create a temporary Data Service
    Optional<DataServiceMeta> dataServiceMeta = transMeta.map( DataServiceMeta::new );
    if ( rowLimit != null && dataServiceMeta.isPresent() ) {
      dataServiceMeta.get().setRowLimit( Integer.parseInt( rowLimit ) );
    }
    dataServiceMeta.ifPresent( configure( dataServiceName, stepName ) );

    return dataServiceMeta.orElse( null );
  }

  private Consumer<DataServiceMeta> configure( String name, String step ) {
    return dataServiceMeta -> {
      dataServiceMeta.setStepname( step );
      dataServiceMeta.setName( name );
      PushDownOptimizationMeta pushDownMeta = new PushDownOptimizationMeta();
      pushDownMeta.setStepName( step );
      pushDownMeta.setType( cacheFactory.createPushDown() );
      dataServiceMeta.setPushDownOptimizationMeta( Collections.singletonList( pushDownMeta ) );
      dataServiceMeta.setUserDefined( false );
    };
  }

  private TransMetaLoader loadFromRepository() {
    return Optional.ofNullable( repositoryLocator )
      // Try to load repository
      .map( KettleRepositoryLocator::getRepository ).flatMap( Optional::ofNullable )
      // If available, attempt to load transformation
      .map( repository -> (TransMetaLoader) fileAndPath -> loadFromRepository( repository, fileAndPath ) )
      // Otherwise defer
      .orElse( fileAndPath -> null );
  }

  private static TransMeta loadFromRepository( Repository repository, String filePath ) throws KettleException {
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
    return buildTransient( filePath, stepName, null );
  }

  public static String buildTransient( String filePath, String stepName, Integer rowLimit ) {
    return PREFIX + encode( filePath ) + DELIMITER + encode( stepName ) + ( rowLimit == null ? ""
      : DELIMITER + encode( rowLimit.toString() ) );
  }

  private static String encode( String value ) {
    return Base64.getEncoder().encodeToString( value.getBytes( StandardCharsets.UTF_8 ) );
  }

  private static String decode( String value ) {
    return new String( Base64.getDecoder().decode( value ), StandardCharsets.UTF_8 );
  }

  public String[] splitTransient( String dataServiceName ) {
    return dataServiceName.replace( PREFIX, "" ).split( DELIMITER );
  }

  @Override public List<String> getDataServiceNames() {
    return new ArrayList<>();
  }

  @Override public List<DataServiceMeta> getDataServices( com.google.common.base.Function<Exception, Void> logger ) {
    return new ArrayList<>();
  }

  private interface TransMetaLoader {
    TransMeta load( String pathAndName ) throws KettleException;

    default Optional<TransMeta> tryLoad( String pathAndName ) {
      try {
        return Optional.ofNullable( load( pathAndName ) );
      } catch ( KettleException e ) {
        return Optional.empty();
      }
    }
  }
}
