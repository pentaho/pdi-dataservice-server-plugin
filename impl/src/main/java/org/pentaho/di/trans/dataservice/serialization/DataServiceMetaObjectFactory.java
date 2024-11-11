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


package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Throwables;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used by the EmbeddedMetaStoreFactory to deserialize PushDownFactories.
 */
public class DataServiceMetaObjectFactory implements IMetaStoreObjectFactory {

  private final List<PushDownFactory> factories;

  public DataServiceMetaObjectFactory( List<PushDownFactory> factories ) {
    this.factories = factories;
  }

  @Override public Object instantiateClass( final String className, Map<String, String> objectContext ) throws
    MetaStoreException {
    for ( PushDownFactory factory : factories ) {
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
