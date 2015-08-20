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

import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;

import java.util.List;
import java.util.Map;

/**
 * Defines the UI and layout for forms that populate the PushDownOptDialog.
 * There are multiple forms of push down optimization possible, and the
 * PushDownOptDialog will allow selecting from a list of optimizations, each
 * with different form elements and layouts.
 */
public interface PushDownOptTypeForm {

  /**
   * Lays out and populates form elements within composite, based on the
   * contents of optimizationMeta.
   */
  void populateForm( Composite composite, PropsUI props, TransMeta transMeta );

  void setValues( PushDownOptimizationMeta optimizationMeta, TransMeta transMeta );

  Map<String, String> getMissingFormElements();

  /**
   * Apply any user-entered form values to optimizationMeta.
   * @param optimizationMeta
   */
  void applyOptimizationParameters( PushDownOptimizationMeta optimizationMeta );

}
