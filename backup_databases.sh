
if [ ! -d "backups" ]; then
    mkdir backups
fi
current_time=$(date "+%Y%m%d")
zip backups/db_backup_${current_time}.zip *.db
# Delete backups older than 7 days
find backups -mtime +7 -exec rm -f {} \;
