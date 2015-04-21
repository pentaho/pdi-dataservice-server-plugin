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

package com.pentaho.di.trans.dataservice.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.List;

public class PushDownOptDialog {

  public static final int TEXT_WIDTH = 250;
  private static final int DIALOG_WIDTH = 691;
  private static final int DIALOG_HEIGHT = 522;
  private final PropsUI props;
  private final TransMeta transMeta;
  private PushDownOptimizationMeta optimizationMeta;
  private final List<PushDownFactory> pushDownFactories;
  protected final Shell parent;

  private int returnCode = SWT.CANCEL;

  private static final Class<?> PKG = PushDownOptDialog.class;

  public PushDownOptDialog( Shell parent, PropsUI props, TransMeta transMeta, PushDownOptimizationMeta optMeta,
                            List<PushDownFactory> pushDownFactories ) {
    this.props = props;
    this.transMeta = transMeta;
    this.parent = parent;
    this.optimizationMeta = optMeta;
    this.pushDownFactories = ImmutableList.copyOf( pushDownFactories );
  }

  public int open() {
    Display display = parent.getDisplay();
    Shell shell = new Shell( display, SWT.SHELL_TRIM );

    layoutDialog( shell );

    setShellPos( shell );
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return returnCode;
  }

  private void layoutDialog( final Shell shell ) {
    props.setLook( shell );

    shell.setText( BaseMessages.getString( PKG, "PushDownOptDialog.PushDownOpt.Label" ) );
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

    final Text nameText = new Text( shell, SWT.BORDER );
    props.setLook( nameText );
    nameText.setText( optimizationMeta.getName() );
    FormData fd_nameText = new FormData();
    fd_nameText.right = new FormAttachment( nameLabel, TEXT_WIDTH, SWT.RIGHT );
    fd_nameText.top = new FormAttachment( 0, 10 );
    fd_nameText.left = new FormAttachment( nameLabel, Const.MARGIN );
    nameText.setLayoutData( fd_nameText );

    Label typeLabel = new Label( shell, SWT.NONE );
    props.setLook( typeLabel );
    typeLabel.setAlignment( SWT.RIGHT );
    FormData fd_lblType = new FormData();
    fd_lblType.top = new FormAttachment( nameLabel, 17 );
    fd_lblType.right = new FormAttachment( nameLabel, 0, SWT.RIGHT );
    typeLabel.setLayoutData( fd_lblType );
    typeLabel.setText( BaseMessages.getString( PKG, "PushDownOptDialog.OptimizationMethod.Label" ) );

    Combo optimizationMethodCombo = new Combo( shell, SWT.NONE );
    props.setLook( optimizationMethodCombo );
    FormData fd_typeCombo = new FormData();
    fd_typeCombo.right = fd_nameText.right;
    fd_typeCombo.top = new FormAttachment( nameText, Const.MARGIN );
    fd_typeCombo.left = fd_nameText.left;
    optimizationMethodCombo.setLayoutData( fd_typeCombo );

    final TypePlaceholder typePlaceholder = new TypePlaceholder( shell, optimizationMethodCombo, SWT.NONE );
    props.setLook( typePlaceholder );
    FormData fd_typePlaceholder = new FormData();
    fd_typePlaceholder.top = new FormAttachment( optimizationMethodCombo, 34 );
    fd_typePlaceholder.left = new FormAttachment( 0, 10 );
    fd_typePlaceholder.bottom = new FormAttachment( 90, 0 );
    fd_typePlaceholder.right = new FormAttachment( 100, -10 );
    typePlaceholder.setLayoutData( fd_typePlaceholder );

    typePlaceholder.select( optimizationMeta.getType() );

    Button okButton = new Button( shell, SWT.NONE );
    props.setLook( okButton );
    okButton.setText( BaseMessages.getString( PKG, "PushDownOptDialog.OK.Label" ) );
    okButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        List<String> missingFormElements = Lists.newArrayList();
        PushDownOptTypeForm pushDownOptTypeForm = typePlaceholder.getSelectedTypeForm();
        missingFormElements.addAll( pushDownOptTypeForm.getMissingFormElements() );
        if ( nameText.getText().trim().isEmpty() ) {
          missingFormElements.add( BaseMessages.getString( PKG, "PushDownOptDialog.MissingName.Message" ) );
        }
        if ( missingFormElements.isEmpty() ) {
          optimizationMeta.setName( nameText.getText() );
          pushDownOptTypeForm.applyOptimizationParameters( optimizationMeta );
          returnCode = SWT.OK;
          shell.dispose();
        } else {
          StringBuilder errors = new StringBuilder().append( "\n\n" );
          for ( String error : missingFormElements ) {
            errors.append( "- " ).append( error ).append( "\n" );
          }
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setText( BaseMessages.getString( PKG, "PushDownOptDialog.MissingFields.Title" ) );
          mb.setMessage( BaseMessages.getString( PKG, "PushDownOptDialog.MissingFields.Message" )
            + errors.toString() );
          mb.open();
        }
      }
    } );

    Button cancelButton = new Button( shell, SWT.NONE );
    props.setLook( cancelButton );
    cancelButton.setText( BaseMessages.getString( PKG, "PushDownOptDialog.Cancel.Label" ) );
    cancelButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        shell.dispose();
      }
    } );

    Button editButton = new Button( shell, SWT.NONE );
    props.setLook( editButton );
    editButton.setText( "Edit Step" );
    editButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        PushDownOptimizationMeta pushDownOptimizationMeta = new PushDownOptimizationMeta();
        PushDownOptTypeForm typeForm = typePlaceholder.getSelectedTypeForm();
        typeForm.applyOptimizationParameters( pushDownOptimizationMeta );
        StepMeta step = transMeta.findStep( pushDownOptimizationMeta.getStepName() );

        if ( step != null ) {
          Spoon.getInstance().editStep( transMeta, step );
        }
      }
    } );

    FormData okFormData = new FormData();
    FormData editFormData = new FormData();
    FormData cancelFormData = new FormData();

    okFormData.top = new FormAttachment( typePlaceholder, Const.MARGIN * 2 );
    okFormData.right = new FormAttachment( editButton, Const.MARGIN * -2 );
    editFormData.top = okFormData.top;
    editFormData.right = new FormAttachment( 50, editButton.computeSize( SWT.DEFAULT, SWT.DEFAULT ).x / 2 );
    cancelFormData.top = okFormData.top;
    cancelFormData.left = new FormAttachment( editButton, Const.MARGIN * 2 );

    okButton.setLayoutData( okFormData );
    editButton.setLayoutData( editFormData );
    cancelButton.setLayoutData( cancelFormData );
  }

  private void setShellPos( Shell shell ) {
    int centerX = parent.getBounds().x + parent.getBounds().width / 2;
    int centerY = parent.getBounds().y + parent.getBounds().height / 2;
    shell.setLocation( centerX - shell.getBounds().width / 2,
      centerY - shell.getBounds().height / 2 );
  }

  private class TypePlaceholder extends Composite {
    final StackLayout layout;
    final PushDownOptTypeForm[] typeForms;
    final Combo typeSelector;

    TypePlaceholder( Shell shell, final Combo typeSelector, int i ) {
      super( shell, i );
      this.typeSelector = typeSelector;
      typeSelector.addSelectionListener( new SelectionAdapter() {
        @Override public void widgetSelected( SelectionEvent selectionEvent ) {
          select( typeSelector.getSelectionIndex() );
        }
      } );

      layout = new StackLayout();
      setLayout( layout );

      String[] typeNames = new String[pushDownFactories.size()];
      typeForms = new PushDownOptTypeForm[pushDownFactories.size()];
      for ( int n = 0; n < pushDownFactories.size(); n++ ) {
        PushDownFactory pushDownFactory = pushDownFactories.get( n );

        typeNames[n] = pushDownFactory.getName();
        typeForms[n] = pushDownFactory.createPushDownOptTypeForm();

        Composite typeForm = new Composite( this, SWT.NONE );
        props.setLook( typeForm );
        typeForm.setLayout( new FormLayout() );
        typeForms[n].populateForm( typeForm, props, transMeta );
      }
      typeSelector.setItems( typeNames );
    }

    void select( int i ) {
      typeSelector.select( i );
      Control control = getChildren()[i];
      if ( layout.topControl != control ) {
        layout.topControl = control;
        layout();
      }
      typeForms[i].setValues( optimizationMeta, transMeta );
    }

    void select( PushDownType type ) {
      for ( int i = 0; type != null && i < pushDownFactories.size(); i++ ) {
        if ( pushDownFactories.get( i ).getType().isInstance( type ) ) {
          select( i );
          return;
        }
      }
      select( 0 );
    }

    PushDownOptTypeForm getSelectedTypeForm() {
      return typeForms[typeSelector.getSelectionIndex()];
    }
  }
}
