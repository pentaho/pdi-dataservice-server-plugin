package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import org.pentaho.di.core.listeners.ContentChangedListener;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaChangeListenerInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

public class SynchronizationService implements ContentChangedListener, StepMetaChangeListenerInterface {
  static Class<TransOpenedExtensionPointPlugin> PKG = TransOpenedExtensionPointPlugin.class;
  final DataServiceDelegate delegate;

  public SynchronizationService( DataServiceDelegate delegate ) {
    this.delegate = delegate;
  }

  @Override public void contentChanged( Object parentObject ) {
  }

  @Override public void contentSafe( Object parentObject ) {
    delegate.sync( (TransMeta) parentObject, syncErrors() );
  }

  @Override public void onStepChange( TransMeta transMeta, StepMeta oldMeta, StepMeta newMeta ) {
    if ( Objects.equal( oldMeta.getName(), newMeta.getName() ) ) {
      return;
    }

    try {
      DataServiceMeta dataService = delegate.getDataServiceByStepName( transMeta, oldMeta.getName() );
      if ( dataService != null ) {
        dataService.setStepname( newMeta.getName() );
        delegate.save( dataService );
      }
    } catch ( MetaStoreException e ) {
      delegate.getLogChannel().logError( e.getMessage(), e );
    }
  }

  Function<Exception, Void> syncErrors() {
    return new Function<Exception, Void>() {
      @Override public Void apply( Exception e ) {
        String message = e.getMessage();
        if ( e instanceof DataServiceAlreadyExistsException ) {
          DataServiceMeta dataService = ( (DataServiceAlreadyExistsException) e ).getDataServiceMeta();
          delegate.getDisplay().syncExec( suggestEdit( dataService, message ) );
        }
        if ( e instanceof UndefinedDataServiceException ) {
          DataServiceMeta dataService = ( (UndefinedDataServiceException) e ).getDataServiceMeta();
          delegate.getDisplay().syncExec( suggestRemove( dataService, message ) );
        }

        delegate.getLogChannel().logError( message, e );
        return null;
      }
    };
  }

  public Runnable suggestEdit( final DataServiceMeta dataService, final String message ) {
    return new Runnable() {
      @Override public void run() {
        delegate.suggestEdit( dataService, BaseMessages.getString( PKG, "Messages.SaveError.Title" ),
          message + "\n" + BaseMessages.getString( PKG, "Messages.SaveError.Edit" ) );
      }
    };
  }

  private Runnable suggestRemove( final DataServiceMeta dataService, final String message ) {
    return new Runnable() {
      @Override public void run() {
        boolean remove = delegate.showPrompt( BaseMessages.getString( PKG, "Messages.SaveError.Title" ),
          message + "\n" + BaseMessages.getString( PKG, "Messages.SaveError.Remove" ) );

        if ( remove ) {
          delegate.removeDataService( dataService );
        }
      }
    };
  }

  public void install( TransMeta transMeta ) {
    List<ContentChangedListener> listeners = transMeta.getContentChangedListeners();
    if ( FluentIterable.from( listeners ).filter( SynchronizationService.class ).isEmpty() ) {
      transMeta.addContentChangedListener( this );
      transMeta.addStepChangeListener( this );
    }
  }
}
