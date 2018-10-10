/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui;

import org.eclipse.swt.widgets.Listener;
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
  private TableView tableView;

  public DataServiceTestResults( DataServiceMeta dataService,
                                 Composite container ) throws KettleStepException {
    this.transMeta = dataService.getServiceTrans();
    this.container = container;
    rowMeta = new RowMeta();
  }

  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  public void load( List<Object[]> rows, Listener tableKeyListener ) {
    tableView = initTableView( rows.size(), tableKeyListener );

    for ( Object[] rowData : rows ) {
      TableItem item = new TableItem( tableView.table, SWT.NONE );
      applyRowDataToTableItem( rowData, item );
    }

    tableView.setRowNums();
    tableView.setShowingConversionErrorsInline( true );
    tableView.optWidth( true );

    container.layout();
  }

  private TableView initTableView( int rowSize, Listener tableKeyListener ) {
    for ( Control control : container.getChildren() ) {
      control.dispose();
    }
    TableView tableView =
      new TableView( transMeta, container, SWT.NONE,
        rowMetaToColumnInfo(), rowSize, true, null,
        PropsUI.getInstance() );
    tableView.table.setItemCount( 0 );
    tableView.table.addListener( SWT.KeyDown, tableKeyListener );
    return tableView;
  }

  public void updateTableView( List<Object[]> rows ) {
    int itemCount = tableView.table.getItemCount();
    for ( int i = 0; i < rows.size(); i++ ) {
      TableItem item = i < itemCount
          ? tableView.table.getItem( i )
          : new TableItem( tableView.table, SWT.NONE );
      applyRowDataToTableItem( rows.get( i ), item );
    }
    tableView.table.setItemCount( rows.size() );
    tableView.setRowNums();
  }

  private void applyRowDataToTableItem( Object[] rowData, TableItem item ) {
    assert rowData.length == rowMeta.size();
    for ( int colNr = 0; colNr < rowMeta.size(); colNr++ ) {
      String cellText = getCellTextFromObj( rowData[colNr], rowMeta.getValueMeta( colNr ) );
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
