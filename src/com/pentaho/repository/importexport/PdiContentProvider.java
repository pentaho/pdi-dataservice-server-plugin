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

package com.pentaho.repository.importexport;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.NamedParamsDefault;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PentahoSystem;


public class PdiContentProvider implements IPdiContentProvider {

  private Log log = LogFactory.getLog( PdiContentProvider.class );

  IUnifiedRepository unifiedRepository = PentahoSystem.get( IUnifiedRepository.class, null );

  @Override
  public boolean hasUserParameters( String kettleFilePath ) {

    if ( !StringUtils.isEmpty( kettleFilePath ) ) {

      RepositoryFile file = unifiedRepository.getFile( kettleFilePath );

      if ( file != null ) {

        try {

          return hasUserParameters( getMeta( file ) );

        } catch ( KettleException e ) {
          log.error( e );
        }
      }
    }

    return false;
  }

  @Override
  public String[] getUserParameters( String kettleFilePath ) {

    List<String> userParams = new ArrayList<String>();

    if ( !StringUtils.isEmpty( kettleFilePath ) ) {

      RepositoryFile file = unifiedRepository.getFile( kettleFilePath );

      if ( file != null ) {

        try {

          NamedParams np = getMeta( file );

          if ( !isEmpty( np = filterUserParameters( np ) ) ) {

            return np.listParameters();
          }

        } catch ( KettleException e ) {
          log.error( e );
        }
      }
    }

    return userParams.toArray( new String[] {} );
  }

  private NamedParams filterUserParameters( NamedParams params ) {

    NamedParams userParams = new NamedParamsDefault();

    if ( !isEmpty( params ) ) {

      for ( String paramName : params.listParameters() ) {

        if ( isUserParameter( paramName ) ) {
          try {
            userParams.addParameterDefinition( paramName, StringUtils.EMPTY, StringUtils.EMPTY );
          } catch ( DuplicateParamException e ) {
            // ignore
          }
        }
      }
    }

    return params;
  }

  private NamedParams getMeta( RepositoryFile file ) throws KettleException {

    NamedParams meta = null;

    if ( file != null ) {

      String extension = FilenameUtils.getExtension( file.getName() );
      Repository repo = PDIImportUtil.connectToRepository( null );

      if ( "ktr".equalsIgnoreCase( extension ) ) {

        meta = new TransMeta( convertTransformation( file.getId() ), repo, true, null, null );

      } else if ( "kjb".equalsIgnoreCase( extension ) ) {

        meta = new JobMeta( convertJob( file.getId() ), repo, null );

      }
    }

    return meta;
  }

  private InputStream convertTransformation( Serializable fileId ) {
    return new StreamToTransNodeConverter( unifiedRepository ).convert( fileId );
  }

  private InputStream convertJob( Serializable fileId ) {
    return new StreamToJobNodeConverter( unifiedRepository ).convert( fileId );
  }

  private boolean isUserParameter( String paramName ) {

    if ( !StringUtils.isEmpty( paramName ) ) {
      // prevent rendering of protected/hidden/system parameters
      if( paramName.startsWith( IPdiContentProvider.PROTECTED_PARAMETER_PREFIX ) ){
        return false;
      }
    }
    return true;
  }

  private boolean hasUserParameters( NamedParams params ) {
    return !isEmpty( filterUserParameters( params ) );
  }

  private boolean isEmpty( NamedParams np ) {
    return np == null || np.listParameters() == null || np.listParameters().length == 0;
  }
}
