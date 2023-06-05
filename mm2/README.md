You will need to download mm2 binary and coins file into this folder, and create an MM2.json file.

- Use `update_mm2.sh` to get the latest mm2 binary release (linux)
- Use `update_coins.sh` to get the latest coins file (this will also be periodically once a day while running the API)
- Use `update_db.sh` to rsync the latest db file from the server. You may need to edit the script to point to a server you have access to. This is only needed for local testing - in prod the db file should already be present and up to date.
- Use `init.sh` to update the coins, start the mm2 API and pipe runtime logs to a file. This is only needed for local testing - in prod the API should already be running and managed by a systemd service.