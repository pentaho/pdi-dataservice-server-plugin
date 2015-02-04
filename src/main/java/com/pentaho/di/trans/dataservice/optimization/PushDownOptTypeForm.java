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

import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;

import java.util.List;

/**
 * Defines the UI and layout for forms that populate the PushDownOptDialog.
 * There are multiple forms of push down optimization possible, and the
 * PushDownOptDialog will allow selecting from a list of optimizations, each
 * with different form elements and layouts.
 */
public interface PushDownOptTypeForm {

  /**
   * @return the name to be displayed as the optimization type in the dialog
   */
  String getName();

  /**
   * Lays out and populates form elements within composite, based on the
   * contents of optimizationMeta.
   */
  void populateForm( Composite composite, PropsUI props,
                     TransMeta transMeta, PushDownOptimizationMeta optimizationMeta );

  boolean isFormValid();

  public List<String> getMissingFormElements();

  /**
   * Apply any user-entered form values to optimizationMeta.
   * @param optimizationMeta
   */
  void applyOptimizationParameters( PushDownOptimizationMeta optimizationMeta );

}
