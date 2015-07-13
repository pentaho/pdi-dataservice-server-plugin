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

package org.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.TransMeta;

import java.util.Collection;

/**
 * @author nhudak
 */
public interface AutoOptimizationService {
  Collection<PushDownOptimizationMeta> apply( TransMeta transMeta, DataServiceMeta dataServiceMeta );
  Collection<Class<? extends PushDownType>> getProvidedOptimizationTypes();
}
