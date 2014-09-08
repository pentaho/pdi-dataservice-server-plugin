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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PushDownOptDialog extends Dialog {

  private static final int DIALOG_WIDTH = 691;
  private static final int DIALOG_HEIGHT = 522;
  private final PropsUI props;
  private final TransMeta transMeta;
  private PushDownOptimizationMeta optimizationMeta;
  protected Shell parent;
  private static Text nameText;

  private Button okButton;

  private int returnCode = SWT.CANCEL;

  private static final Class<?> PKG = PushDownOptDialog.class;

  private final List<PushDownOptTypeForm> pushDownTypes =
    Collections.unmodifiableList(
      Arrays.asList( (PushDownOptTypeForm) new ParamGenOptForm() ) );

  private Combo optimizationMethodCombo;


  public PushDownOptDialog( Shell parent, PropsUI props, TransMeta transMeta, PushDownOptimizationMeta optMeta ) {
    super( parent );
    this.props = props;
    this.transMeta = transMeta;
    this.parent = parent;
    this.optimizationMeta = optMeta;
  }

  public int open() {
    layoutDialog();
    return returnCode;
  }



  private void layoutDialog() {
    Display display = parent.getDisplay();
    final Shell shell = new Shell( display );



    props.setLook( shell );

    shell.setText( BaseMessages.getString( PKG, "PushDownOptDialog.PushDownOpt.Label" )  );
    shell.setLayout( new FormLayout() );

    shell.setSize( DIALOG_WIDTH, DIALOG_HEIGHT );

    Label nameLabel = new Label( shell, SWT.NONE );
    props.setLook( nameLabel );
    nameLabel.setAlignment( SWT.RIGHT );
    FormData fd_nameLabel = new FormData();
    fd_nameLabel.top = new FormAttachment( 0, 10 );
    fd_nameLabel.left = new FormAttachment( 30, 0 );
    nameLabel.setLayoutData( fd_nameLabel );
    nameLabel.setText( BaseMessages.getString( PKG, "PushDownOptDialog.Name.Label" ) );

    nameText = new Text( shell, SWT.BORDER );
    props.setLook( nameText );
    nameText.setText( optimizationMeta.getName() );
    FormData fd_nameText = new FormData();
    fd_nameText.right = new FormAttachment( nameLabel, 251, SWT.RIGHT );
    fd_nameText.top = new FormAttachment( 0, 10 );
    fd_nameText.left = new FormAttachment( nameLabel, 6 );
    nameText.setLayoutData( fd_nameText );

    Label typeLabel = new Label( shell, SWT.NONE );
    props.setLook( typeLabel );
    typeLabel.setAlignment( SWT.RIGHT );
    FormData fd_lblType = new FormData();
    fd_lblType.top = new FormAttachment( nameLabel, 17 );
    fd_lblType.right = new FormAttachment( nameLabel, 0, SWT.RIGHT );
    typeLabel.setLayoutData( fd_lblType );
    typeLabel.setText( BaseMessages.getString( PKG, "PushDownOptDialog.OptimizationMethod.Label" ) );

    optimizationMethodCombo = new Combo( shell, SWT.NONE );
    props.setLook( optimizationMethodCombo );
    FormData fd_typeCombo = new FormData();
    fd_typeCombo.right = new FormAttachment( nameText, 0, SWT.RIGHT );
    fd_typeCombo.top = new FormAttachment( nameText, 6 );
    fd_typeCombo.left = new FormAttachment( nameText, 0, SWT.LEFT );
    optimizationMethodCombo.setLayoutData( fd_typeCombo );
    optimizationMethodCombo.setItems( getTypeNames() );
    optimizationMethodCombo.select( 0 );

    Composite typePlaceholder = new Composite( shell, SWT.NONE );
    props.setLook( typePlaceholder );
    typePlaceholder.setLayout( new FormLayout() );
    FormData fd_typePlaceholder = new FormData();
    fd_typePlaceholder.top = new FormAttachment( optimizationMethodCombo, 34 );
    fd_typePlaceholder.left = new FormAttachment( 0, 10 );
    fd_typePlaceholder.bottom = new FormAttachment( 90, 0 );
    fd_typePlaceholder.right = new FormAttachment( 100, -10 );

    typePlaceholder.setLayoutData( fd_typePlaceholder );

    okButton = new Button( shell, SWT.NONE );
    props.setLook( okButton );
    FormData fd_btnOk = new FormData();
    fd_btnOk.top = new FormAttachment( typePlaceholder, Const.MARGIN * 2 );
    fd_btnOk.left = new FormAttachment( 50, -40 );
    okButton.setLayoutData( fd_btnOk );
    okButton.setText( BaseMessages.getString( PKG, "PushDownOptDialog.OK.Label" ) );
    okButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        if ( !isFormValid() ) {
          StringBuilder errors = new StringBuilder().append( "\n\n" );
          for ( String error : getMissingFormElements() ) {
            errors.append( "*" ).append( error ).append( "\n" );
          }
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setText( BaseMessages.getString( PKG, "PushDownOptDialog.MissingFields.Title" ) );
          mb.setMessage( BaseMessages.getString( PKG, "PushDownOptDialog.MissingFields.Message" )
            + errors.toString() );
          mb.open();
        } else {
          initializeOptimizationMeta();
          returnCode = SWT.OK;
          shell.dispose();
        }
      }
    } );

    Button cancelButton = new Button( shell, SWT.NONE );
    props.setLook( cancelButton );
    FormData fd_btnCancel = new FormData();
    fd_btnCancel.top = fd_btnOk.top;
    fd_btnCancel.left = new FormAttachment( okButton, Const.MARGIN * 2 );
    cancelButton.setLayoutData( fd_btnCancel );
    cancelButton.setText( BaseMessages.getString( PKG, "PushDownOptDialog.Cancel.Label" ) );
    cancelButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        shell.dispose();
      }
    } );
    layoutTypeForm( typePlaceholder,
      optimizationMethodCombo.getSelectionIndex() );

    setShellPos( shell );
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private void setShellPos( Shell shell ) {
    int centerX = parent.getBounds().x + parent.getBounds().width / 2;
    int centerY = parent.getBounds().y + parent.getBounds().height / 2;
    shell.setLocation( centerX - shell.getBounds().width / 2,
      centerY - shell.getBounds().height / 2 );
  }

  private String[] getTypeNames() {
    String[] typeNames = new String[ pushDownTypes.size() ];
    for ( int i = 0; i < pushDownTypes.size(); i++ ) {
      typeNames[ i ] = pushDownTypes.get( i ).getName();
    }
    return typeNames;
  }

  private void initializeOptimizationMeta() {
    optimizationMeta.setName( nameText.getText() );
    getPushDownOptTypeForm().applyOptimizationParameters( optimizationMeta );
  }

  private boolean isFormValid() {
    return getMissingFormElements().size() == 0;
  }

  private List<String> getMissingFormElements() {
    List<String> errors = getPushDownOptTypeForm().getMissingFormElements();
    if ( nameText.getText().trim().length() == 0 ) {
      errors.add( BaseMessages.getString( PKG, "PushDownOptDialog.MissingName.Message" ) );
    }
    return errors;
  }

  private void layoutTypeForm( Composite typePlaceholder, int item ) {
    getPushDownOptTypeForm()
      .populateForm( typePlaceholder, props, transMeta, optimizationMeta );
  }

  private PushDownOptTypeForm getPushDownOptTypeForm() {
    final int selectionIndex = optimizationMethodCombo.getSelectionIndex();
    return pushDownTypes.get( selectionIndex < 0 ? 0 : selectionIndex );
  }


}
