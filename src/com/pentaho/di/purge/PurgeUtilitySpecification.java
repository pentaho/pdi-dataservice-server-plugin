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
package com.pentaho.di.purge;

import java.util.Date;

import org.apache.log4j.Level;

/**
 * @author tkafalas
 */
public class PurgeUtilitySpecification {
  String path;
  boolean purgeFiles; //If set, remove files in total rather then revisions
  boolean purgeRevisions; //If set, purge all revisions for a file.  Ignored if purgeFiles is set.
  boolean sharedObjects; //If set, purge shared objects as well
  int versionCount = -1; //if not equal to -1, keep only the newest versionCount versions of a file
  Date beforeDate; //if not null, delete all revisions dated before beforeDate
  String fileFilter = "*"; //File filter used by Tree call
  Level logLevel = Level.INFO;
  
  public PurgeUtilitySpecification() {
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  public boolean isPurgeFiles() {
    return purgeFiles;
  }

  public void setPurgeFiles( boolean purgeFiles ) {
    this.purgeFiles = purgeFiles;
  }

  public boolean isPurgeRevisions() {
    return purgeRevisions;
  }

  public void setPurgeRevisions( boolean purgeRevisions ) {
    this.purgeRevisions = purgeRevisions;
  }

  public int getVersionCount() {
    return versionCount;
  }

  public void setVersionCount( int versionCount ) {
    this.versionCount = versionCount;
  }

  public Date getBeforeDate() {
    return beforeDate;
  }

  public void setBeforeDate( Date beforeDate ) {
    this.beforeDate = beforeDate;
  }

  public String getFileFilter() {
    return fileFilter;
  }

  public void setFileFilter( String fileFilter ) {
    this.fileFilter = fileFilter;
  }

  public boolean isSharedObjects() {
    return sharedObjects;
  }

  public void setSharedObjects( boolean sharedObjects ) {
    this.sharedObjects = sharedObjects;
  }

  public Level getLogLevel() {
    return logLevel;
  }

  public void setLogLevel( Level logLevel ) {
    this.logLevel = logLevel;
  }

}
