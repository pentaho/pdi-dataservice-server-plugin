/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Function;

/**
 * @author nhudak
 */
public interface MetaStoreElement {
  String getName();

  void setName( String name );

  Function<MetaStoreElement, String> getName = new Function<MetaStoreElement, String>() {
    @Override public String apply( MetaStoreElement input ) {
      return input.getName();
    }
  };
}
