/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.logging.MetricsPainter;
import org.pentaho.di.core.metrics.MetricsDuration;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceTestController;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.SWTGC;

/**
 * @author nhudak
 */
public class DataServiceTestMetrics {
  private final Canvas canvas;
  private volatile Image image;

  private final ScrolledComposite metricsComposite;
  private List<MetricsDuration> durations = Collections.emptyList();

  public static final Comparator<MetricsDuration> METRICS_COMPARATOR = new Comparator<MetricsDuration>() {
    @Override
    public int compare( MetricsDuration o1, MetricsDuration o2 ) {
      return o1.getDate().compareTo( o2.getDate() );
    }
  };

  public DataServiceTestMetrics( Composite composite ) {
    composite.setLayout( new FillLayout() );
    metricsComposite = new ScrolledComposite( composite, SWT.H_SCROLL | SWT.V_SCROLL );
    metricsComposite.setExpandHorizontal( true );
    metricsComposite.setExpandVertical( true );

    canvas = new Canvas( metricsComposite, SWT.NONE );
    metricsComposite.setContent( canvas );
    metricsComposite.setMinSize( 100, 100 );

    metricsComposite.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        if ( image != null ) {
          image.dispose();
        }
      }
    } );

    canvas.addPaintListener( new PaintListener() {
      public void paintControl( final PaintEvent event ) {
        if ( Const.isRunningOnWebspoonMode() ) {
          refreshImage( event.gc );
        } else {
          event.gc.drawImage( refreshImage(), 0, 0 );
        }
      }
    } );
  }

  public synchronized void display( List<MetricsDuration> durations ) {
    if ( Const.isEmpty( durations ) ) {
      // In case of an empty durations or null there is nothing to draw
      durations = Collections.emptyList();
    } else {
      // Sort the metrics.
      Collections.sort( durations, METRICS_COMPARATOR );
    }

    if ( image != null ) {
      image.dispose();
      image = null; // Clear image cache
    }

    this.durations = durations;
    canvas.redraw();
  }

  private synchronized void refreshImage( GC gc2 ) {
    Rectangle bounds = metricsComposite.getBounds();

    String metricsMessage = BaseMessages.getString( DataServiceTestController.PKG, "DataServiceTest.MetricsPlaceholder" );
    org.eclipse.swt.graphics.Point textExtent = new GC( canvas ).textExtent( metricsMessage );
    if ( durations.isEmpty() ) {
      bounds.height = textExtent.y * 2;
      gc2.drawText( metricsMessage, ( bounds.width - textExtent.x ) / 2, textExtent.y / 2 );
    } else {
      // Adjust height to fit all bars
      int barHeight = textExtent.y + 5;
      bounds.height = Math.max( 20 + durations.size() * ( barHeight + 5 ), bounds.height );
      SWTGC gc = new SWTGC( gc2, new Point( bounds.width, bounds.height ), PropsUI.getInstance().getIconSize() );
      MetricsPainter painter = new MetricsPainter( gc, barHeight );
      painter.paint( durations );
      gc.dispose();
    }
  }

  private synchronized Image refreshImage() {
    Display device = metricsComposite.getDisplay();
    Rectangle bounds = metricsComposite.getBounds();

    if ( image != null ) {
      Rectangle imageBounds = image.getBounds();
      // Check if image in cache fits
      if ( imageBounds.width == bounds.width && imageBounds.height >= bounds.height ) {
        return image;
      } else {
        // Need to regenerate
        image.dispose();
        image = null;
      }
    }

    String metricsMessage = BaseMessages.getString( DataServiceTestController.PKG, "DataServiceTest.MetricsPlaceholder" );
    org.eclipse.swt.graphics.Point textExtent = new GC( canvas ).textExtent( metricsMessage );
    if ( durations.isEmpty() ) {
      bounds.height = textExtent.y * 2;
      image = new Image( device, bounds.width, bounds.height );
      GC gc = new GC( image );
      gc.drawText( metricsMessage, ( bounds.width - textExtent.x ) / 2, textExtent.y / 2 );
    } else {
      // Adjust height to fit all bars
      int barHeight = textExtent.y + 5;
      bounds.height = Math.max( 20 + durations.size() * ( barHeight + 5 ), bounds.height );
      SWTGC gc = new SWTGC( device, new Point( bounds.width, bounds.height ), PropsUI.getInstance().getIconSize() );
      MetricsPainter painter = new MetricsPainter( gc, barHeight );
      painter.paint( durations );
      image = (Image) gc.getImage();
      gc.dispose();
    }

    metricsComposite.setMinHeight( bounds.height );
    metricsComposite.layout();

    return image;
  }

  public void dispose() {
    metricsComposite.dispose();
    if ( canvas != null ) {
      canvas.dispose();
    }
  }
}
