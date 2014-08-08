/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2014 Pentaho Corporation..  All rights reserved.
*/
package com.pentaho.di.revision;

import org.pentaho.di.core.util.Assert;
import org.pentaho.di.repository.pur.PurObjectRevision;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.web.http.api.resources.FileResource;

import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RevisionResourceTest {

  RevisionResource revisionResource;

  RepositoryFile mockRepositoryFile;

  private static final String MOCK_FILE_PATH = ":mock:file:path";
  private static final String MOCK_FILE_ID = "0123456789";

  private static final String MOCK_VERSION_ID_1 = "0123456789";
  private static final String MOCK_VERSION_AUTHOR_1 = "Admin";
  private static final String MOCK_VERSION_MESSAGE_1 = "Version message 1";

  /**
   *
   * @throws Exception
   */
  @org.junit.Before
  public void setUp() throws Exception {
    mockRepositoryFile = mock( RepositoryFile.class );
    when( mockRepositoryFile.getId() ).thenReturn( MOCK_FILE_ID );
    IUnifiedRepository mockRepository = mock( IUnifiedRepository.class );

    when( mockRepository.getFile( FileResource.idToPath(MOCK_FILE_PATH) ) ).thenReturn( mockRepositoryFile );
    when( mockRepository.getVersionSummaries( MOCK_FILE_ID ) ).thenReturn( getMockVersionSummaries() );

    revisionResource = new RevisionResource( mockRepository );
  }

  /**
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testDoGetVersions() throws Exception {
    Response response = revisionResource.doGetVersions( MOCK_FILE_PATH );
    Object entity = response.getEntity();

    // Yeah this gets weird: List, wrapped in a Response, wrapped in GenericEnttiy
    List<PurObjectRevision> revisionList = (List<PurObjectRevision>) ((GenericEntity) entity).getEntity();

    Assert.assertTrue( revisionList.size() == 1 );
    Assert.assertTrue( revisionList.get(0).getLogin().equals( MOCK_VERSION_AUTHOR_1 ));
  }

  /**
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testGetVersioningEnabled() throws Exception {
    Assert.assertTrue( Boolean.parseBoolean( revisionResource.getVersioningEnabled().getEntity().toString() ) );
  }

  /**
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testGetVersionCommentsEnabled() throws Exception {
    Assert.assertTrue( Boolean.parseBoolean( revisionResource.getVersioningEnabled().getEntity().toString() ) );
  }

  /**
   * Return mock list of version summaries
   *
   * @return
   */
  private List<VersionSummary> getMockVersionSummaries(){
    List<VersionSummary> versionSummaries = new ArrayList<VersionSummary>();

    VersionSummary versionSummary1 = new VersionSummary(
        MOCK_VERSION_ID_1,
        MOCK_FILE_ID,
        false,
        new Date(),
        MOCK_VERSION_AUTHOR_1,
        MOCK_VERSION_MESSAGE_1,
        new ArrayList<String>()
    );

    versionSummaries.add( versionSummary1 );

    return versionSummaries;
  }
}