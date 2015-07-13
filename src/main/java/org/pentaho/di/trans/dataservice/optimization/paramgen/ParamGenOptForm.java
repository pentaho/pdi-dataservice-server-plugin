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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;

import java.util.ArrayList;

public class ParamGenOptForm implements PushDownOptTypeForm {
  private static final Class<?> PKG = ParamGenOptForm.class;

  private final ParameterGenerationFactory serviceProvider;

  private TextVar paramNameText;
  private TableView definitionTable;
  private List stepList;

  public ParamGenOptForm( ParameterGenerationFactory serviceProvider ) {
    this.serviceProvider = serviceProvider;
  }

  public java.util.List<String> getMissingFormElements() {
    java.util.List<String> errors = new ArrayList<String>();
    if ( !atLeastOneMappingExists() ) {
      errors.add( BaseMessages.getString( PKG, "ParamGenOptForm.MissingFieldMapping.Message" ) );
    }
    String paramName = paramNameText.getText();
    if ( paramName == null || paramName.trim().length() == 0 ) {
      errors.add( BaseMessages.getString( PKG, "ParamGenOptForm.MissingParamName.Message" ) );
    }
    if ( !( stepList.getSelection().length == 1
      && stepList.getSelection()[ 0 ].trim().length() > 0 ) ) {
      errors.add( BaseMessages.getString( PKG, "ParamGenOptForm.MissingStep.Message" ) );
    }
    return errors;
  }

  private boolean atLeastOneMappingExists() {
    for ( int i = 0; i < definitionTable.getItemCount(); i++ ) {
      String source = definitionTable.getItem( i, 2 );
      String target = definitionTable.getItem( i, 1 );
      if ( source.trim().length() > 0 && target.trim().length() > 0 ) {
        return true;
      }
    }
    return false;
  }


  @Override public void setValues( PushDownOptimizationMeta optimizationMeta, TransMeta transMeta ) {
    stepList.setItems( getSupportedSteps( transMeta ) );
    setSelectedStep( optimizationMeta.getStepName() );

    if ( optimizationMeta.getType() instanceof ParameterGeneration ) {
      ParameterGeneration paramType = (ParameterGeneration) optimizationMeta.getType();
      setSourceTargetValues( paramType );
      paramNameText.setText( paramType.getParameterName() );
    }
  }

  @Override
  public void populateForm( Composite composite, PropsUI props, final TransMeta transMeta ) {
    Group paramGenGroup = new Group( composite, SWT.SHADOW_IN );
    props.setLook( paramGenGroup );
    paramGenGroup.setText( BaseMessages.getString( PKG, "ParamGenOptForm.ParamGen.Label" ) );
    paramGenGroup.setLayout( new FormLayout() );
    FormData paramGenFormData = new FormData();
    paramGenFormData.top = new FormAttachment( 0 );
    paramGenFormData.left = new FormAttachment( 0 );
    paramGenFormData.right = new FormAttachment( 100 );
    paramGenFormData.bottom = new FormAttachment( 100 );

    paramGenGroup.setLayoutData( paramGenFormData );

    Label stepNameLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( stepNameLabel );
    FormData labelStepNameFormData = new FormData();
    labelStepNameFormData.top = new FormAttachment( 0, Const.FORM_MARGIN * 3 );
    labelStepNameFormData.left = new FormAttachment( 0, Const.FORM_MARGIN * 2 );

    stepNameLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.StepName.Label" ) );
    stepNameLabel.setLayoutData( labelStepNameFormData );
    stepList = new List( paramGenGroup, SWT.BORDER );
    props.setLook( stepList );

    FormData fdStepList = new FormData();
    fdStepList.top = new FormAttachment( stepNameLabel, Const.FORM_MARGIN );
    fdStepList.width = 250;
    fdStepList.height = 100;
    fdStepList.left = labelStepNameFormData.left;
    stepList.setLayoutData( fdStepList );

    Label definitionLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( definitionLabel );
    FormData fdDefinition = new FormData();
    fdDefinition.top = new FormAttachment( stepList, Const.FORM_MARGIN * 3 );
    fdDefinition.left = new FormAttachment( 0, Const.FORM_MARGIN * 2 );
    definitionLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.Definition.Label" ) );
    definitionLabel.setLayoutData( fdDefinition );

    Label parameterNameLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( parameterNameLabel );
    FormData fdParamNameLabel = new FormData();
    fdParamNameLabel.left = new FormAttachment( stepList, Const.FORM_MARGIN * 9 );
    fdParamNameLabel.top = new FormAttachment( 0, Const.FORM_MARGIN * 3 );
    parameterNameLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.ParamName.Label" ) );
    parameterNameLabel.setLayoutData( fdParamNameLabel );

    paramNameText = new TextVar( transMeta, paramGenGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( paramNameText );

    FormData paramNameFormData = new FormData();
    paramNameFormData.left = new FormAttachment( stepList, Const.FORM_MARGIN * 9 );
    paramNameFormData.top = new FormAttachment( parameterNameLabel, Const.FORM_MARGIN );
    paramNameFormData.right = new FormAttachment( 100, -Const.FORM_MARGIN * 2 );
    paramNameText.setLayoutData( paramNameFormData );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ParamGenOptForm.SourceOutputField.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ParamGenOptForm.SourceStepField.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false )
        //  NO FILTER ALLOWED COMBO YET
        //        ,
        //        new ColumnInfo(
        //          "Filter Allowed",
        //          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{ "Yes", "No" }, false )
      };

    definitionTable = new TableView(
      transMeta, paramGenGroup, SWT.SINGLE | SWT.BORDER | SWT.FULL_SELECTION, colinf, 1, null, props );
    props.setLook( definitionTable );
    FormData fdDefTable = new FormData();
    fdDefTable.top = new FormAttachment( definitionLabel, Const.FORM_MARGIN );
    fdDefTable.left = new FormAttachment( 0, Const.FORM_MARGIN * 2 );
    fdDefTable.right = new FormAttachment( 100, -Const.FORM_MARGIN * 2 );
    fdDefTable.bottom = new FormAttachment( 100, -Const.FORM_MARGIN * 3 );
    definitionTable.setLayoutData( fdDefTable );
  }

  private void setSourceTargetValues( ParameterGeneration optimization ) {
    definitionTable.table.removeAll();
    for ( SourceTargetFields fields : optimization.getFieldMappings() ) {
      TableItem item;
      item = new TableItem( definitionTable.table, SWT.NONE );
      item.setText( 1, fields.getTargetFieldName() );
      item.setText( 2, fields.getSourceFieldName() );
    }
  }

  private void setSelectedStep( String stepName ) {
    if ( stepName == null ) {
      return;
    }
    for ( int i = 0; i < stepList.getItems().length; i++ ) {
      if ( stepName.equals( stepList.getItem( i ) ) ) {
        stepList.setSelection( i );
        return;
      }
    }
  }

  @Override
  public void applyOptimizationParameters( PushDownOptimizationMeta optimizationMeta ) {
    if ( stepList.getSelection().length == 1 ) {
      optimizationMeta.setStepName( stepList.getSelection()[ 0 ] );
    }
    optimizationMeta.setType( getParameterGeneration() );
  }

  private ParameterGeneration getParameterGeneration() {
    ParameterGeneration parameterGeneration = serviceProvider.createPushDown();
    for ( int i = 0; i < definitionTable.getItemCount(); i++ ) {
      String source = definitionTable.getItem( i, 2 );
      String target = definitionTable.getItem( i, 1 );
      if ( source.trim().length() > 0 && target.trim().length() > 0 ) {
        parameterGeneration.createFieldMapping( source, target );
      }
    }
    parameterGeneration.setParameterName( cleanseParamName( paramNameText.getText() ) );
    return parameterGeneration;
  }

  private String cleanseParamName( String name ) {
    String paramName = Const.nullToEmpty( name ).trim();
    if ( paramName.startsWith( "${" )
      && paramName.endsWith( "}" ) ) {
      paramName = paramName.substring( 2, paramName.length() - 1 );
    }
    return paramName;
  }

  private String[] getSupportedSteps( TransMeta transMeta ) {
    java.util.List<String> stepNames = new ArrayList<String>();
    for ( StepMeta step : transMeta.getSteps() ) {
      if ( serviceProvider.supportsStep( step ) ) {
        stepNames.add( step.getName() );
      }
    }
    return stepNames.toArray( new String[ stepNames.size() ] );
  }

}

