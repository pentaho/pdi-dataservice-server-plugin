/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.net.MediaType;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.www.BaseCartePlugin;

import javax.cache.Cache;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Set;

/**
 * This servlet allows a user to clear the data service cache
 *
 * @author nhudak
 */
@CarteServlet(
  id = "ds_clearServiceCache",
  name = "PDI Data Service: ServiceCache reset",
  description = "Clear a data service Cache" )
public class ResetCacheServlet extends BaseCartePlugin {
  private static final String NAME_PARAMETER = "name";
  private final ServiceCacheFactory factory;

  public ResetCacheServlet( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  private static final String CONTEXT_PATH = "/clearDataServiceCache";

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override public void handleRequest( CarteRequest request ) throws IOException {
    Collection<String> names = request.getParameters().get( NAME_PARAMETER );
    if ( names == null || names.isEmpty() ) {
      request.respond( 400 ).withMessage( NAME_PARAMETER + " not specified" );
      return;
    }

    final Set<Cache> cacheSet = FluentIterable.from( names )
      .transform( new Function<String, Cache>() {
        @Override public Cache apply( String name ) {
          return factory.getCache( name ).orNull();
        }
      } )
      .filter( Predicates.notNull() )
      .toSet();

    for ( Cache cache : cacheSet ) {
      cache.clear();
    }

    request
      .respond( 200 )
      .with( MediaType.PLAIN_TEXT_UTF_8.toString(), new WriterResponse() {
        @Override public void write( PrintWriter writer ) throws IOException {
          if ( cacheSet.isEmpty() ) {
            writer.println( "No matching caches to flush." );
          }
          for ( Cache cache : cacheSet ) {
            writer.println( "Cleared cache: " + cache.getName() );
          }
          writer.println( "Done" );
        }
      } );
  }
}
