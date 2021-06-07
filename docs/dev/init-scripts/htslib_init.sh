#!/bin/bash
sleep 10
wget https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2
tar xjvf htslib-1.9.tar.bz2
cd htslib-1.9
./configure
make
make install
sleep 10
