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

package com.pentaho.di.trans.dataservice.optimization;

import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.IMetaStoreObjectFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author nhudak
 */
public class PushDownOptimizationObjectFactory implements IMetaStoreObjectFactory {

  private static final String TYPE_PARAMETER_PREFIX = "TYPE_PARAMETER_";

  @Override public Object instantiateClass( String className, Map<String, String> context ) throws MetaStoreException {
    Class<?> typeClass;
    try {
      // TODO Classes loaded from class loader, consider creating a new plugin type
      typeClass = Class.forName( className );
      if ( PushDownType.class.isAssignableFrom( typeClass ) ) {
        PushDownType type = (PushDownType) typeClass.newInstance();
        // Rehydrate parameters from context
        type.loadParameters( removeKeyPrefix( TYPE_PARAMETER_PREFIX, context ) );
        return type;
      } else {
        // Load a native type
        return typeClass.newInstance();
      }
    } catch ( ReflectiveOperationException e ) {
      throw new MetaStoreException( e );
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    Map<String, String> map = new HashMap<String, String>();
    if ( pluginObject instanceof PushDownType ) {
      PushDownType type = (PushDownType) pluginObject;
      // Save parameters to context
      map.putAll( addKeyPrefix( TYPE_PARAMETER_PREFIX, type.saveParameters() ) );
    }
    return map;
  }

  private <T> Map<String, T> addKeyPrefix( String prefix, Map<String, T> map ) {
    Map<String, T> prefixedMap = new HashMap<String, T>();
    for ( String key : map.keySet() ) {
      prefixedMap.put( prefix + key, map.get( key ) );
    }
    return prefixedMap;
  }

  private <T> Map<String, T> removeKeyPrefix( String prefix, Map<String, T> prefixedMap ) {
    Map<String, T> map = new HashMap<String, T>();
    for ( String key : prefixedMap.keySet() ) {
      if ( key.startsWith( prefix ) ) {
        map.put( key.substring( prefix.length() ), prefixedMap.get( key ) );
      }
    }
    return map;
  }
}
