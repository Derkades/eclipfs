METASERVER="enter metaserver address"
USERNAME="enter username"
PASSWORD="enter password"
LOCATION="enter preferred location for transfers"

export DEBUG="1"

clear
umount /mnt/eclipfs
cd src
python3 -u mount.py "$METASERVER" /mnt/eclipfs -o "user=$USERNAME,pass=$PASSWORD,loc=$LOCATION"
