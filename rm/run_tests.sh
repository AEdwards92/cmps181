#!/bin/bash

for testcase in *rmtest_[01][0-9] ; do
    ./$testcase 
done
