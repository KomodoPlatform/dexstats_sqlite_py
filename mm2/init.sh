#!/bin/bash

./update_coins.sh
./update_mm2.sh
./mm2 > ./mm2.log &
source userpass
echo $userpass
sleep 5
./version.sh
tail -f ./mm2.log