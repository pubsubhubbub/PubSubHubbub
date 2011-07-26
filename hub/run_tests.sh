#!/bin/bash

for test_file in $(ls *_test.py ../nonstandard/*_test.py)
do
  echo -e "========== Running $test_file"
  ./$test_file
  if [ "$?" -ne "0" ]; then
    echo "Died in $test_file"
    exit 1
  fi
done
