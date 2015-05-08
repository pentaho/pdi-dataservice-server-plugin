/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.Iterables;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;

import java.util.ArrayList;

public class ServiceCacheOptForm implements PushDownOptTypeForm {
  private static final Class<?> PKG = ServiceCacheOptForm.class;

  private final ServiceCacheFactory factory;

  private List templateList;

  public ServiceCacheOptForm( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  public java.util.List<String> getMissingFormElements() {
    java.util.List<String> errors = new ArrayList<String>();
    if ( !( templateList.getSelection().length == 1 && templateList.getSelection()[0].trim().length() > 0 ) ) {
      errors.add( BaseMessages.getString( PKG, "ServiceCacheOptForm.MissingTemplate.Message" ) );
    }
    return errors;
  }

  @Override public void setValues( PushDownOptimizationMeta optimizationMeta, TransMeta transMeta ) {
    templateList.setItems( Iterables.toArray( factory.getTemplates(), String.class ) );
    setSelectedItem( templateList, optimizationMeta.getType() instanceof ServiceCache ?
      ( (ServiceCache) optimizationMeta.getType() ).getTemplateName() : null );
  }

  @Override
  public void populateForm( Composite composite, PropsUI props, final TransMeta transMeta ) {
    Group cacheOptGroup = new Group( composite, SWT.SHADOW_IN );
    props.setLook( cacheOptGroup );
    cacheOptGroup.setText( BaseMessages.getString( PKG, "ServiceCacheOptForm.Label" ) );
    cacheOptGroup.setLayout( new FormLayout() );
    FormData cacheFormData = new FormData();
    cacheFormData.top = new FormAttachment( 0 );
    cacheFormData.left = new FormAttachment( 0 );
    cacheFormData.right = new FormAttachment( 100 );
    cacheFormData.bottom = new FormAttachment( 100 );

    cacheOptGroup.setLayoutData( cacheFormData );

    Label templateNameLabel = new Label( cacheOptGroup, SWT.NONE );
    props.setLook( templateNameLabel );
    FormData templateNameFormData = new FormData();
    templateNameFormData.top = new FormAttachment( 5 );
    templateNameFormData.left = new FormAttachment( 0, Const.MARGIN );
    templateNameLabel.setText( BaseMessages.getString( PKG, "ServiceCacheOptForm.TemplateName.Label" ) );
    templateNameLabel.setLayoutData( templateNameFormData );

    templateList = new List( cacheOptGroup, SWT.BORDER );
    props.setLook( templateList );
    FormData fdTemplateList = new FormData();
    fdTemplateList.top = new FormAttachment( templateNameLabel, Const.MARGIN * 2 );
    fdTemplateList.width = 200;
    fdTemplateList.height = 100;
    fdTemplateList.left = templateNameFormData.left;
    templateList.setLayoutData( fdTemplateList );
  }

  private void setSelectedItem( List list, String item ) {
    for ( int i = 0; item != null && i < list.getItemCount(); i++ ) {
      if ( item.equals( list.getItem( i ) ) ) {
        list.setSelection( i );
        return;
      }
    }

    if ( list.getItemCount() > 0 ) {
      list.setSelection( 0 );
    }
  }

  @Override
  public void applyOptimizationParameters( PushDownOptimizationMeta optimizationMeta ) {
    ServiceCache pushDown = factory.createPushDown();
    optimizationMeta.setType( pushDown );
  }

}

