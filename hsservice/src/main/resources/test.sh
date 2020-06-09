#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`

SERVER_NAME=`basename $DEPLOY_DIR`

echo "BIN_DIR=${BIN_DIR}"
echo "DEPLOY_DIR=${DEPLOY_DIR}"
echo "SERVER_NAME=${SERVER_NAME}"