#! /usr/bin/bash
protoc --python_out=./script hebe.proto 
protoc --java_out=./src hebe.proto 
cd src/ && ./build_hebe.sh  

