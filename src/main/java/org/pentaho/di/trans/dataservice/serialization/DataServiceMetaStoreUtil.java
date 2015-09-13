/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.pentaho.caching.api.Constants;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;
import org.pentaho.metastore.persist.MetaStoreFactory;

import javax.cache.Cache;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static org.pentaho.di.i18n.BaseMessages.getString;
import static org.pentaho.metastore.util.PentahoDefaults.NAMESPACE;

public class DataServiceMetaStoreUtil {
  private static final Class<DataServiceMetaStoreUtil> PKG = DataServiceMetaStoreUtil.class;
  private static final HashFunction hashFunction = Hashing.goodFastHash( Integer.SIZE );
  protected final DataServiceContext context;
  protected final Cache<Integer, String> stepCache;

  public DataServiceMetaStoreUtil( DataServiceContext context, Cache<Integer, String> cache ) {
    this.context = context;
    this.stepCache = cache;
  }

  protected DataServiceMetaStoreUtil( DataServiceMetaStoreUtil metaStoreUtil ) {
    this( metaStoreUtil.context, metaStoreUtil.stepCache );
  }

  public static DataServiceMetaStoreUtil create( DataServiceContext context ) {
    return new DataServiceMetaStoreUtil( context, initCache( context.getCacheManager() ) );
  }

  private static Cache<Integer, String> initCache( PentahoCacheManager cacheManager ) {
    String name = DataServiceMetaStoreUtil.class.getName() + UUID.randomUUID().toString();
    return cacheManager.getTemplates().get( Constants.DEFAULT_TEMPLATE ).
      createCache( name, Integer.class, String.class );
  }

  public DataServiceMeta getDataService( String serviceName, Repository repository, IMetaStore metaStore )
    throws MetaStoreException {
    ServiceTrans transReference = getServiceTransFactory( metaStore ).loadElement( serviceName );
    if ( transReference == null ) {
      throw new MetaStoreException( MessageFormat.format( "Data Service {0} not found", serviceName ) );
    }

    final AtomicReference<Exception> loadException = new AtomicReference<Exception>();
    Optional<TransMeta> transMeta = FluentIterable.from( transReference.getReferences() ).
      transform( createTransMetaLoader( repository, new Function<Exception, Void>() {
        @Override public Void apply( Exception e ) {
          loadException.compareAndSet( null, e );
          return null;
        }
      } ) ).
      transform( Suppliers.<TransMeta>supplierFunction() ).
      firstMatch( Predicates.notNull() );

    if ( transMeta.isPresent() ) {
      return getDataService( serviceName, transMeta.get() );
    } else {
      throw new MetaStoreException( MessageFormat.format( "Failed to load Data Service {0}", serviceName ),
        loadException.get() );
    }
  }

  public DataServiceMeta getDataService( String serviceName, TransMeta serviceTrans ) throws MetaStoreException {
    DataServiceMeta dataServiceMeta = getDataServiceFactory( serviceTrans ).loadElement( serviceName );
    if ( dataServiceMeta == null ) {
      throw new MetaStoreException( MessageFormat.format( "Data Service {0} not found in transformation {1}",
        serviceName, serviceTrans.getName() ) );
    }
    return dataServiceMeta;
  }

  public Iterable<DataServiceMeta> getDataServices( final Repository repository, IMetaStore metaStore,
                                                    final Function<Exception, Void> exceptionHandler ) {
    MetaStoreFactory<ServiceTrans> serviceTransFactory = getServiceTransFactory( metaStore );

    List<ServiceTrans> serviceTransList;
    try {
      serviceTransList = serviceTransFactory.getElements();
    } catch ( Exception e ) {
      exceptionHandler.apply( e );
      serviceTransList = Collections.emptyList();
    }

    Map<ServiceTrans.Reference, Supplier<TransMeta>> transMetaMap = FluentIterable.from( serviceTransList ).
      transformAndConcat( new Function<ServiceTrans, Iterable<ServiceTrans.Reference>>() {
        @Override public Iterable<ServiceTrans.Reference> apply( ServiceTrans serviceTrans ) {
          return serviceTrans.getReferences();
        }
      } ).
      toMap( createTransMetaLoader( repository, exceptionHandler ) );

    List<DataServiceMeta> dataServices = Lists.newArrayListWithExpectedSize( serviceTransList.size() );
    for ( ServiceTrans serviceTrans : serviceTransList ) {
      Optional<TransMeta> transMeta = FluentIterable.from( serviceTrans.getReferences() ).
        transform( Functions.forMap( transMetaMap ) ).
        transform( Suppliers.<TransMeta>supplierFunction() ).
        firstMatch( Predicates.notNull() );
      if ( transMeta.isPresent() ) {
        try {
          dataServices.add( getDataService( serviceTrans.getName(), transMeta.get() ) );
        } catch ( Exception e ) {
          exceptionHandler.apply( e );
        }
      }
    }
    return dataServices;
  }

  private Function<ServiceTrans.Reference, Supplier<TransMeta>> createTransMetaLoader( final Repository repository,
                                                                                       final Function<? super Exception, ?> exceptionHandler ) {
    return new Function<ServiceTrans.Reference, Supplier<TransMeta>>() {
      @Override public Supplier<TransMeta> apply( final ServiceTrans.Reference reference ) {
        return Suppliers.memoize( new Supplier<TransMeta>() {
          @Override public TransMeta get() {
            try {
              return reference.load( repository );
            } catch ( KettleException e ) {
              exceptionHandler.apply( e );
              return null;
            }
          }
        } );
      }
    };
  }

  public Iterable<DataServiceMeta> getDataServices( TransMeta transMeta ) {
    try {
      return getDataServiceFactory( transMeta ).getElements();
    } catch ( MetaStoreException e ) {
      getLogChannel().logError( "Unable to list data services for " + transMeta.getName(), e );
      return ImmutableList.of();
    }
  }

  public List<String> getDataServiceNames( TransMeta transMeta )
    throws MetaStoreException {
    return getDataServiceFactory( transMeta ).getElementNames();
  }

  public List<String> getDataServiceNames( IMetaStore metaStore )
    throws MetaStoreException {
    return getServiceTransFactory( metaStore ).getElementNames();
  }

  public DataServiceMeta getDataServiceByStepName( TransMeta transMeta, String stepName ) {
    Set<Integer> cacheKeys = createCacheKeys( transMeta, stepName );
    for ( Map.Entry<Integer, String> entry : stepCache.getAll( cacheKeys ).entrySet() ) {
      String serviceName = entry.getValue();
      if ( serviceName.isEmpty() ) {
        // Step is marked as not having a Data Service
        return null;
      }
      // Check if Data Service is still valid
      DataServiceMeta dataServiceMeta;
      try {
        dataServiceMeta = getDataService( serviceName, transMeta );
      } catch ( MetaStoreException e ) {
        dataServiceMeta = null;
      }
      if ( dataServiceMeta != null && dataServiceMeta.getStepname().equals( stepName ) ) {
        return dataServiceMeta;
      } else {
        stepCache.remove( entry.getKey(), serviceName );
      }
    }
    // Look up from embedded metastore
    for ( DataServiceMeta dataServiceMeta : getDataServices( transMeta ) ) {
      if ( dataServiceMeta.getStepname().equalsIgnoreCase( stepName ) ) {
        return dataServiceMeta;
      }
    }
    // Data service not found on step, store negative result in the cache
    stepCache.putAll( Maps.asMap( cacheKeys, Functions.constant( "" ) ) );
    return null;
  }

  public DataServiceMeta checkDefined( DataServiceMeta dataServiceMeta ) throws UndefinedDataServiceException {
    if ( Strings.isNullOrEmpty( dataServiceMeta.getName() ) ) {
      throw new UndefinedDataServiceException( dataServiceMeta, getString( PKG, "Messages.SaveError.NameMissing" ) );
    }

    if ( Strings.isNullOrEmpty( dataServiceMeta.getStepname() ) ) {
      throw new UndefinedDataServiceException( dataServiceMeta, getString( PKG, "Messages.SaveError.StepMissing" ) );
    }

    if ( dataServiceMeta.getServiceTrans().findStep( dataServiceMeta.getStepname() ) == null ) {
      throw new UndefinedDataServiceException( dataServiceMeta,
        getString( PKG, "Messages.SaveError.StepNotFound", dataServiceMeta.getStepname() ) );
    }

    return dataServiceMeta;
  }

  public DataServiceMeta checkConflict( DataServiceMeta dataServiceMeta, String ignored )
    throws MetaStoreException, DataServiceAlreadyExistsException {
    TransMeta serviceTrans = dataServiceMeta.getServiceTrans();

    // Ensure this output step does not already have a data service
    DataServiceMeta stepConflict = getDataServiceByStepName( serviceTrans, dataServiceMeta.getStepname() );
    if ( stepConflict != null && !stepConflict.getName().equals( ignored ) ) {
      throw new DataServiceAlreadyExistsException( dataServiceMeta,
        getString( PKG, "Messages.SaveError.StepConflict", stepConflict.getStepname(), stepConflict.getName() ) );
    }

    String name = dataServiceMeta.getName();
    // If name hasn't changed, look no further
    if ( name.equals( ignored ) ) {
      return dataServiceMeta;
    }

    // Scan local trans and meta store for conflict
    if ( getDataServiceNames( serviceTrans ).contains( name ) ) {
      throw new DataServiceAlreadyExistsException( dataServiceMeta );
    }
    // Scan MetaStore for conflict
    if ( getDataServiceNames( serviceTrans.getMetaStore() ).contains( name ) ) {
      throw new DataServiceAlreadyExistsException( dataServiceMeta );
    }

    return dataServiceMeta;
  }

  public void save( DataServiceMeta dataService ) throws MetaStoreException {
    TransMeta transMeta = checkNotNull( dataService.getServiceTrans(), "Service trans not defined for data service" );

    getLogChannel().logBasic( MessageFormat.format( "Saving ''{0}'' to ''{1}''",
      dataService.getName(), transMeta.getName() ) );

    // Save to embedded MetaStore
    getDataServiceFactory( transMeta ).saveElement( dataService );
    transMeta.setChanged();
  }

  public void removeDataService( DataServiceMeta dataService ) {
    TransMeta transMeta = dataService.getServiceTrans();
    try {
      getDataServiceFactory( transMeta ).deleteElement( dataService.getName() );
      for ( Integer key : createCacheKeys( transMeta, dataService.getStepname() ) ) {
        stepCache.replace( key, dataService.getName(), "" );
      }
      transMeta.setChanged();
    } catch ( MetaStoreException e ) {
      getLogChannel().logBasic( e.getMessage() );
    }
  }

  public void sync( TransMeta transMeta, Function<? super Exception, ?> exceptionHandler ) {
    final MetaStoreFactory<ServiceTrans> serviceTransFactory = getServiceTransFactory( transMeta.getMetaStore() );

    final Set<ServiceTrans.Reference> references;
    final Set<String> defined;
    final Map<String, DataServiceMeta> dataServices;
    final Map<String, ServiceTrans> published;

    try {
      references = ImmutableSet.copyOf( ServiceTrans.references( transMeta ) );

      dataServices = Maps.uniqueIndex( getDataServices( transMeta ), MetaStoreElement.getName );

      List<ServiceTrans> serviceTransElements = serviceTransFactory.getElements();
      defined = FluentIterable.from( serviceTransElements ).transform( MetaStoreElement.getName ).toSet();
      published = FluentIterable.from( serviceTransElements )
        .filter( new Predicate<ServiceTrans>() {
          @Override public boolean apply( ServiceTrans serviceTrans ) {
            return Iterables.any( serviceTrans.getReferences(), in( references ) );
          }
        } )
        .uniqueIndex( MetaStoreElement.getName );
    } catch ( MetaStoreException e ) {
      exceptionHandler.apply( e );
      return;
    }

    Map<String, ServiceTrans> toDelete = Maps.filterKeys( published, not( in( dataServices.keySet() ) ) );
    Map<String, DataServiceMeta> nameConflicts = Maps.filterKeys( dataServices, and(
      in( defined ),
      not( in( published.keySet() ) )
    ) );
    Map<String, DataServiceMeta> toSave = Maps.filterKeys( dataServices, not( in( nameConflicts.keySet() ) ) );

    for ( DataServiceMeta dataServiceMeta : toSave.values() ) {
      try {
        serviceTransFactory.saveElement( ServiceTrans.create( checkDefined( dataServiceMeta ) ) );
      } catch ( Exception e ) {
        exceptionHandler.apply( e );
      }
    }
    for ( String name : toDelete.keySet() ) {
      try {
        serviceTransFactory.deleteElement( name );
      } catch ( MetaStoreException e ) {
        exceptionHandler.apply( e );
      }
    }
    for ( DataServiceMeta dataServiceMeta : nameConflicts.values() ) {
      exceptionHandler.apply( new DataServiceAlreadyExistsException( dataServiceMeta ) );
    }
  }

  protected MetaStoreFactory<ServiceTrans> getServiceTransFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<>( ServiceTrans.class, metaStore, NAMESPACE );
  }

  protected MetaStoreFactory<DataServiceMeta> getDataServiceFactory( final TransMeta transMeta ) {
    return new MetaStoreFactory<DataServiceMeta>( DataServiceMeta.class, transMeta.getEmbeddedMetaStore(), NAMESPACE ) {

      {
        setObjectFactory( new DataServiceMetaObjectFactory() );
      }

      @Override public DataServiceMeta loadElement( String name ) throws MetaStoreException {
        DataServiceMeta dataServiceMeta = super.loadElement( name );
        if ( dataServiceMeta != null ) {
          dataServiceMeta.setServiceTrans( transMeta );
          stepCache.putAll( createCacheEntries( dataServiceMeta ) );
        }
        return dataServiceMeta;
      }

      @Override public List<DataServiceMeta> getElements() throws MetaStoreException {
        List<DataServiceMeta> elements = super.getElements();
        for ( DataServiceMeta dataServiceMeta : elements ) {
          dataServiceMeta.setServiceTrans( transMeta );
          stepCache.putAll( createCacheEntries( dataServiceMeta ) );
        }
        return elements;
      }

      @Override public void saveElement( DataServiceMeta dataServiceMeta ) throws MetaStoreException {
        super.saveElement( dataServiceMeta );
        stepCache.putAll( createCacheEntries( dataServiceMeta ) );
      }
    };
  }

  static Set<Integer> createCacheKeys( TransMeta transMeta, final String stepName ) {
    HashCode hash = hashFunction.newHasher()
      .putString( transMeta.getName(), Charsets.UTF_8 )
      .putString( stepName, Charsets.UTF_8 )
      .hash();
    return ImmutableSet.of( hash.asInt() );
  }

  static Map<Integer, String> createCacheEntries( DataServiceMeta dataService ) {
    Set<Integer> keys = createCacheKeys( dataService.getServiceTrans(), dataService.getStepname() );
    return Maps.asMap( keys, Functions.constant( dataService.getName() ) );
  }

  public LogChannelInterface getLogChannel() {
    return context.getLogChannel();
  }

  public List<PushDownFactory> getPushDownFactories() {
    return context.getPushDownFactories();
  }

  public Function<Exception, Void> logErrors( final String message ) {
    return new Function<Exception, Void>() {
      @Override public Void apply( Exception e ) {
        getLogChannel().logError( message, e );
        return null;
      }
    };
  }

  private class DataServiceMetaObjectFactory implements IMetaStoreObjectFactory {
    @Override public Object instantiateClass( final String className, Map<String, String> objectContext ) throws
      MetaStoreException {
      for ( PushDownFactory factory : getPushDownFactories() ) {
        if ( factory.getType().getName().equals( className ) ) {
          return factory.createPushDown();
        }
      }
      try {
        return Class.forName( className ).newInstance();
      } catch ( Throwable t ) {
        Throwables.propagateIfPossible( t, MetaStoreException.class );
        throw new MetaStoreException( t );
      }
    }

    @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
      return Collections.emptyMap();
    }

  }
}
