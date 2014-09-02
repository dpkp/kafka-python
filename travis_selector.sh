#!/bin/bash
# This works with the .travis.yml file to select a python version for testing

if [ $1 == "pypy" ]; then
    echo "pypy"
elif [ $1 == "3.4" ]; then
    echo "py34"
elif [ $1 == "3.3" ]; then
    echo "py33"
elif [ $1 == "2.7" ]; then
    echo "py27"
elif [ $1 == "2.6" ]; then
    echo "py26"
else
    echo $1
fi;
