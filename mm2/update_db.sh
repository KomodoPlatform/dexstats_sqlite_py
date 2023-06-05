#!/bin/bash
mkdir -p ../DB/43ec929fe30ee72be42c9162c56dde910a05e50d
cd ../DB/43ec929fe30ee72be42c9162c56dde910a05e50d
rsync atomic@stats-api.atomicdex.io:/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db .
cd -