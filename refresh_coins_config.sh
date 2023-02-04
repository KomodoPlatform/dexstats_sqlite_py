#!/bin/bash

cd /home/atomic/dexstats_sqlite_py
sha_local=($(sha256sum ./coins_config.json))
curl -s https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json --output coins_config.new
sha_remote=($(sha256sum ./coins_config.new))

if [[ $sha_local != $sha_remote ]]; then
  rm coins_config.json && mv coins_config.new coins_config.json
else
  rm coins_config.new
fi

