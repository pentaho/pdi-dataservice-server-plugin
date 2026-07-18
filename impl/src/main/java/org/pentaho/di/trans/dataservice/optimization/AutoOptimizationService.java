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



package org.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.util.Collection;

/**
 * @author nhudak
 */
public interface AutoOptimizationService {
  Collection<PushDownOptimizationMeta> apply( DataServiceMeta dataServiceMeta );
  Collection<Class<? extends PushDownType>> getProvidedOptimizationTypes();
}
