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

package org.pentaho.di.trans.dataservice.ui;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;

import java.util.List;

/**
 * Encapsulation of a TableView for displaying results within
 * the DataServiceTestDialog.
 */
public class DataServiceTestResults {

  private RowMetaInterface rowMeta;
  private final TransMeta transMeta;
  private final Composite container;

  public DataServiceTestResults( DataServiceMeta dataService,
                                 TransMeta transMeta,
                                 Composite container ) throws KettleStepException {
    this.transMeta = transMeta;
    this.container = container;
    rowMeta = new RowMeta();
  }

  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  public void load( List<Object[]> rows ) {
    TableView tableView = initTableView( rows );

    for ( Object[] rowData : rows ) {
      TableItem item = new TableItem( tableView.table, SWT.NONE );
      applyRowDataToTableItem( rowData, item );
    }

    tableView.setRowNums();
    tableView.setShowingConversionErrorsInline( true );
    tableView.optWidth( true );
  }

  private TableView initTableView( List<Object[]> rows ) {
    for ( Control control : container.getChildren() ) {
      control.dispose();
    }
    TableView tableView =
      new TableView( transMeta, container, SWT.NONE,
        rowMetaToColumnInfo(), rows.size(), true, null,
        PropsUI.getInstance() );
    tableView.table.setItemCount( 0 );
    tableView.setSize( container.getSize() );
    return tableView;
  }

  private void applyRowDataToTableItem( Object[] rowData, TableItem item ) {
    assert rowData.length == rowMeta.size();
    for ( int colNr = 0; colNr < rowMeta.size(); colNr++ ) {
      String cellText = getCellTextFromObj( rowData[ colNr ], rowMeta.getValueMeta( colNr ) );
      setCellText( colNr, cellText, item );
    }
  }

  private void setCellText( int colNr, String cellText, TableItem tableItem ) {
    if ( cellText == null ) {
      tableItem.setText( colNr + 1, "<null>" );
      tableItem.setForeground( GUIResource.getInstance().getColorBlue() );
      return;
    }
    tableItem.setForeground( colNr + 1, GUIResource.getInstance().getColorBlack() );
    tableItem.setText( colNr + 1, cellText );
  }

  private String getCellTextFromObj( Object object, ValueMetaInterface valueMeta ) {
    try {
      if ( valueMeta.isStorageBinaryString() ) {
        return valueMeta.getStorageMetadata().getString(
          valueMeta.convertBinaryStringToNativeType( (byte[]) object ) );
      } else {
        return valueMeta.getString( object );
      }
    } catch ( KettleValueException valueException ) {
      return "ERROR: " + valueException.getMessage();
    }
  }

  private ColumnInfo[] rowMetaToColumnInfo() {
    ColumnInfo[] columnInfo = new ColumnInfo[rowMeta.size()];
    for ( int i = 0; i < columnInfo.length; i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      columnInfo[i] = new ColumnInfo( valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, false, true );
      columnInfo[i].setValueMeta( valueMeta );
    }
    return columnInfo;
  }
}
