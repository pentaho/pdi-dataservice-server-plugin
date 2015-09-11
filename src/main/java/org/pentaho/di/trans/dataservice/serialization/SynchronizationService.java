package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.pentaho.di.core.listeners.ContentChangedListener;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaChangeListenerInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

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
        String title = BaseMessages.getString( PKG, "Messages.SaveError.Title" );
        if ( e instanceof DataServiceAlreadyExistsException ) {
          DataServiceMeta dataService = ( (DataServiceAlreadyExistsException) e ).getDataServiceMeta();
          delegate.suggestEdit( dataService, title, e.getMessage() + "\n" + BaseMessages.getString( PKG, "Messages.SaveError.Edit" ) );
        }
        if ( e instanceof UndefinedDataServiceException ) {
          DataServiceMeta dataService = ( (UndefinedDataServiceException) e ).getDataServiceMeta();
          if ( delegate.showPrompt( title, e.getMessage() + "\n" + BaseMessages.getString( PKG, "Messages.SaveError.Remove" ) ) ) {
            delegate.removeDataService( dataService, false );
          }
        }

        delegate.getLogChannel().logError( e.getMessage(), e );
        return null;
      }
    };
  }
}
