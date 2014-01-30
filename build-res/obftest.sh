#!/bin/bash

#/*!
# * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
# *
# * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
# *
# * NOTICE: All information including source code contained herein is, and
# * remains the sole property of Pentaho and its licensors. The intellectual
# * and technical concepts contained herein are proprietary and confidential
# * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
# * patents, or patents in process, and are protected by trade secret and
# * copyright laws. The receipt or possession of this source code and/or related
# * information does not convey or imply any rights to reproduce, disclose or
# * distribute its contents, or to manufacture, use, or sell anything that it
# * may describe, in whole or in part. Any reproduction, modification, distribution,
# * or public display of this information without the express written authorization
# * from Pentaho is strictly prohibited and in violation of applicable laws and
# * international treaties. Access to the source code contained herein is strictly
# * prohibited to anyone except those individuals and entities who have executed
# * confidentiality and non-disclosure agreements or other agreements with Pentaho,
# * explicitly covering such access.
# */

# ${1}: root directory for searching

# Can't use the following line as some grep calls return 1 (which is OK for our purposes)
# set -e

#set -x

# get absolute path for directory containing this script
pushd $(dirname $0)
abs_dir=$(pwd)
popd

# create a work directory
work_dir=$(mktemp -d)
echo "Work directory: ${work_dir}"

# setup JAR patterns
patterns[0]='*pentaho*.jar'
patterns[1]='agile-bi*.jar'
patterns[2]='bi-platform*.jar'
patterns[3]='cda*.jar'
patterns[4]='echo-plugin*.jar'
patterns[5]='flute*.jar'
patterns[6]='gecho-plugin*.jar'
patterns[7]='google-analytics-input-step*.jar'
patterns[8]='json*.jar'
patterns[9]='jtreetable*.jar'
patterns[10]='lib*.jar'
patterns[11]='license-creator*.jar'
patterns[12]='mantle*.jar'
patterns[13]='mondrian*.jar'
patterns[14]='mqleditor*.jar'
patterns[15]='olap4j*.jar'
patterns[16]='pdi*.jar'
patterns[17]='pec*.jar'
patterns[18]='prd*.jar'
patterns[19]='pre-classic-sdk*.jar'
patterns[20]='pur-repository*.jar'
patterns[21]='report-designer*.jar'
patterns[22]='scheduler-plugin*.jar'

# @param fail supply this parameter to exit with failure after cleanup; absence means we will not exit
function cleanup {
  rm -rf ${work_dir}
  if [ $# == 1 ]; then
    echo "FAILED!"
    exit 1
  fi
}

# function that reads file names (one per line) from stdin and unzips them into their own directory
# @param delete file after unzip
function unzip1 {
  while read file1
  do
    dest_dir=$(mktemp -d --tmpdir=${work_dir})
    echo "    Unzipping ${file1} to ${dest_dir}"
    if echo "${file1}" | grep -q "\.tar\.gz$"; then
      tar xvzf ${file1} -C ${dest_dir} >/dev/null 2>/dev/null
    else
      unzip ${file1} -d ${dest_dir} >/dev/null 2>/dev/null
    fi
    if [ $1 == 1 ]; then
      rm ${file1}
    fi
  done
}

echo "- Extracting first-level archives"

archive_patterns[0]='*.jar'
archive_patterns[1]='*.zip'
archive_patterns[2]='*.tar.gz'

for pattern in ${archive_patterns[@]}
do
  echo "  - Unzipping all files matching ${pattern} in ${1}"
  delete_after_unzip=0
  find ${1} -name ${pattern} -print | unzip1 ${delete_after_unzip}
done

for i in {1..3}
do
  echo "- Extracting second-level archives (pass $i})"
  # extract class files from JAR files
  for pattern in ${patterns[@]}
  do
    echo "  - Unzipping all files matching ${pattern} in ${work_dir}"
    delete_after_unzip=1
    find ${work_dir} -name ${pattern} -print | unzip1 ${delete_after_unzip}
  done
done

function count1 {
  while read file1
  do
    return 0
  done
  return 1
}

# fail safe
find ${work_dir} -name "*.class" | count1
if [ $? -eq 1 ]; then
  echo "No class files!"
  cleanup 1
fi

# decompile class files
echo "- Decompiling"
find ${work_dir} -name "*.class" -execdir jad {} >/dev/null 2>/dev/null \;

function grep1 {
  while read file1
  do
    echo ${file1} | grep "PentahoLicense"
    if [ $? -eq 0 ]; then
      echo "  ${file1} failed"
      return 1
    fi
  done
}

echo "- Searching"
# search for blacklisted file names
find ${work_dir} -name "*.jad" | grep1
if [ $? -eq 1 ]; then 
  echo "  - One or more source file names found containing substring PentahoLicense"
  cleanup 1
fi

function grep2 {
  while read file1
  do
    grep "PentahoLicense" ${file1} >/dev/null 2>/dev/null
    if [ $? -eq 0 ]; then
      echo "  ${file1} failed"
      return 1
    fi
  done
}

# search within source for blacklisted strings
find ${work_dir} -name "*.jad" | grep2
if [ $? -eq 1 ] 
  then echo "  - One or more source files found containing substring PentahoLicense"
  cleanup 1
fi

function grep3 {
  while read file1
  do
    grep "VERSION_FOR_LICENSE" ${file1} >/dev/null 2>/dev/null
    if [ $? -eq 0 ]; then
      echo "  ${file1} failed"
      return 1
    fi
  done
}

find ${work_dir} -name "*.jad" | grep3
if [ $? -eq 1 ] 
  then echo "  - One or more source files found containing substring VERSION_FOR_LICENSE"
  cleanup 1
fi

# everything's good
cleanup
echo "PASSED!"
exit 0
