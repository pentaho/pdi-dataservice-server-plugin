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

import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class ParamGenOptForm implements PushDownOptTypeForm {
  private static final Class<?> PKG = ParamGenOptForm.class;
  protected static Text paramNameText;
  protected TableView definitionTable;
  private ParameterGeneration parameterGeneration = new ParameterGeneration();

  private static final java.util.List<String> supportedStepTypes =
    Collections.unmodifiableList(
      Arrays.asList( "TableInput" ) );
  private static final java.util.List<String> supportedDbTypes =
    Collections.unmodifiableList(
      Arrays.asList( "H2" ) );  // Where should this list come from?
  protected List stepList;

  @Override
  public String getName() {
    return parameterGeneration.getTypeName();
  }

  @Override
  public void populateForm( Composite composite, PropsUI props, TransMeta transMeta,
                            PushDownOptimizationMeta optimizationMeta ) {
    layoutWidgets( composite, props, transMeta );
    setInitialValues( optimizationMeta, transMeta );
  }

  @Override
  public boolean isFormValid() {
    String paramName = paramNameText.getText();
    return atLeastOneMappingExists()
      && paramName != null
      && paramName.trim().length() > 0
      && stepList.getSelection().length == 1
      && stepList.getSelection()[ 0 ].trim().length() > 0;
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

  protected void setInitialValues( PushDownOptimizationMeta optimizationMeta, TransMeta transMeta ) {
    ParameterGeneration paramType = (ParameterGeneration) optimizationMeta.getType();
    if ( paramType == null ) {
      paramType = new ParameterGeneration();
      optimizationMeta.setType( paramType );
    }
    setSourceTargetValues( definitionTable, optimizationMeta );

    stepList.setItems( getSupportedSteps( transMeta ) );
    setSelectedStep( stepList, optimizationMeta.getStepName() );

    paramNameText.setText( paramType.getParameterName() );
  }

  private void layoutWidgets( Composite composite, PropsUI props, TransMeta transMeta ) {
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
    labelStepNameFormData.top = new FormAttachment( 5 );

    stepNameLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.StepName.Label" ) );
    stepNameLabel.setLayoutData( labelStepNameFormData );
    stepList = new List( paramGenGroup, SWT.BORDER );
    props.setLook( stepList );

    FormData fdStepList = new FormData();
    fdStepList.top = new FormAttachment( stepNameLabel, Const.MARGIN * 2 );
    fdStepList.width = 200;
    fdStepList.height = 100;
    stepList.setLayoutData( fdStepList );

    Label definitionLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( definitionLabel );
    FormData fdDefinition = new FormData();
    fdDefinition.top = new FormAttachment( stepList, Const.MARGIN * 2 );
    definitionLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.Definition.Label" ) );
    definitionLabel.setLayoutData( fdDefinition );

    Label parameterNameLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( parameterNameLabel );
    parameterNameLabel.setAlignment( SWT.RIGHT );
    FormData fdParamNameLabel = new FormData();
    fdParamNameLabel.left = new FormAttachment( stepList, Const.MARGIN * 4 );
    fdParamNameLabel.top = fdStepList.top;
    fdParamNameLabel.width = 130;
    parameterNameLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.ParamName.Label" ) );
    parameterNameLabel.setLayoutData( fdParamNameLabel );

    Label formLabel = new Label( paramGenGroup, SWT.NONE );
    props.setLook( formLabel );
    formLabel.setAlignment( SWT.RIGHT );
    formLabel.setText( BaseMessages.getString( PKG, "ParamGenOptForm.Form.Label" ) );

    FormData fdFormLabel = new FormData();
    fdFormLabel.top = new FormAttachment( parameterNameLabel, Const.MARGIN * 5 );
    fdFormLabel.left = fdParamNameLabel.left;
    fdFormLabel.width = 130;
    formLabel.setLayoutData( fdFormLabel );

    paramNameText = new Text( paramGenGroup, SWT.BORDER );
    props.setLook( paramNameText );

    FormData paramNameFormData = new FormData();
    paramNameFormData.left = new FormAttachment( parameterNameLabel, Const.MARGIN * 2 );
    paramNameFormData.top = fdStepList.top;
    paramNameFormData.width = 150;
    paramNameText.setLayoutData( paramNameFormData );

    Combo formCombo = new Combo( paramGenGroup, SWT.NONE );
    props.setLook( formCombo );
    formCombo.setItems( availableParamForms() );
    formCombo.select( 0 );
    FormData fdFormCombo = new FormData();
    fdFormCombo.top = fdFormLabel.top;
    fdFormCombo.left = paramNameFormData.left;
    fdFormCombo.width = 160;
    formCombo.setLayoutData( fdFormCombo );

    ColumnInfo[] colinf =
      new ColumnInfo[]{
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
      transMeta, paramGenGroup, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, null, props );
    props.setLook( definitionTable );

    FormData fdDefTable = new FormData();
    fdDefTable.top = new FormAttachment( definitionLabel, Const.MARGIN * 2 );
    fdDefTable.left = new FormAttachment( 0 );
    fdDefTable.right = new FormAttachment( 100 );
    fdDefTable.bottom = new FormAttachment( 100 );
    definitionTable.setLayoutData( fdDefTable );
  }

  private void setSourceTargetValues( TableView definitionTable, PushDownOptimizationMeta optimizationMeta ) {
    boolean firstRow = true;
    for ( SourceTargetFields fields : ( (ParameterGeneration) optimizationMeta.getType() ).getFieldMappings() ) {
      TableItem item;
      if ( firstRow ) {
        item = definitionTable.table.getItem( 0 );
        firstRow = false;
      } else {
        item = new TableItem( definitionTable.table, SWT.NONE );
      }
      item.setText( 1, fields.getTargetFieldName() );
      item.setText( 2, fields.getSourceFieldName() );
    }
  }

  private void setSelectedStep( List stepList, String stepName ) {
    if ( stepName == null ) {
      return;
    }
    for ( int i = 0; i < stepList.getItems().length; i++ ) {
      if ( stepName.equals(stepList.getItem( i ) ) ) {
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
    for ( int i = 0; i < definitionTable.getItemCount(); i++ ) {
      String source = definitionTable.getItem( i, 2 );
      String target = definitionTable.getItem( i, 1 );
      if ( source.trim().length() > 0 && target.trim().length() > 0 ) {
        parameterGeneration.createFieldMapping( source, target );
      }
    }
    parameterGeneration.setParameterName( paramNameText.getText() );
    optimizationMeta.setType( parameterGeneration );
  }

  private String[] availableParamForms() {
    return new String[]{"Where Clause"};
  }

  private String[] getSupportedSteps( TransMeta transMeta ) {
    java.util.List<String> stepNames = new ArrayList<String>();
    for ( StepMeta step : transMeta.getSteps() ) {
      if ( !supportedStepTypes.contains( step.getTypeId() ) ) {
        continue;
      }
      DatabaseMeta[] dbMeta = step.getStepMetaInterface().getUsedDatabaseConnections();
      if ( dbMeta.length == 1
        && dbMeta[ 0 ].getDatabaseInterface() != null
        && supportedDbTypes.contains( dbMeta[ 0 ].getDatabaseInterface().getPluginId() ) ) {
        stepNames.add( step.getName() );
      }
    }
    return stepNames.toArray( new String[ stepNames.size() ] );
  }


}

