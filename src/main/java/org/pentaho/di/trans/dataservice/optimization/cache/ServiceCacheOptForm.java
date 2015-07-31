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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.Iterables;
import org.eclipse.swt.widgets.Control;
import org.pentaho.caching.api.Constants;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
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
import org.pentaho.di.ui.core.widget.TextVar;

import java.util.ArrayList;

public class ServiceCacheOptForm implements PushDownOptTypeForm {
  private static final Class<?> PKG = ServiceCacheOptForm.class;

  private final ServiceCacheFactory factory;

  private List templateList;
  private TextVar timeToLiveText;

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
    templateList.setItems( Iterables.toArray( factory.getTemplateNames(), String.class ) );
    updateForm( optimizationMeta );
  }

  @Override public void populateForm( Composite composite, PropsUI props, final TransMeta transMeta ) {
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

    timeToLiveText = new TextVar( transMeta, cacheOptGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( timeToLiveText );
    FormData fdTimeToLiveText = new FormData();
    fdTimeToLiveText.top = new FormAttachment( templateList, Const.MARGIN * 2 );
    fdTimeToLiveText.left = templateNameFormData.left;
    timeToLiveText.setLayoutData( fdTimeToLiveText );
  }

  private void updateForm( PushDownOptimizationMeta pushDownOptimizationMeta ) {
    String timeToLive = null;
    if ( pushDownOptimizationMeta.getType() instanceof ServiceCache ) {
      ServiceCache serviceCache = (ServiceCache) pushDownOptimizationMeta.getType();
      for ( int i = 0; i < templateList.getItemCount(); i++ ) {
        if ( serviceCache.getTemplateName().equals( templateList.getItem( i ) ) ) {
          templateList.select( i );
          timeToLive = serviceCache.getTimeToLive();
        }
      }
    } else {
      if ( templateList.getItemCount() > 0 ) {
        templateList.select( 0 );
      }
    }

    setPropertyValue( timeToLiveText, templateList.getSelection()[ 0 ], Constants.CONFIG_TTL, timeToLive );
  }

  private void setPropertyValue( Control control, String templateName, String property, String value ) {
    if ( value == null ) {
      value = getPropertyValue( templateName, property );
    }

    if ( control instanceof TextVar ) {
      ( (TextVar) control ).setText( value );
    }
  }

  private String getPropertyValue( String templateName, String propertyName ) {
    return factory.getPropertiesByTemplateName( templateName ).get( propertyName );
  }

  @Override public void applyOptimizationParameters( PushDownOptimizationMeta optimizationMeta ) {
    ServiceCache pushDown = factory.createPushDown();
    if ( templateList.getSelection().length == 1 ) {
      pushDown.setTemplateName( templateList.getSelection()[0] );
      pushDown.setTimeToLive( timeToLiveText.getText() );
    }
    optimizationMeta.setType( pushDown );
  }

}

