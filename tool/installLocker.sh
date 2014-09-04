d=`pwd`
sync=`echo ${d} | sed -e "s/\(.*clean_sync\).*/\1/g"`
p=`find ${sync} | grep mongo_locker.dart`
port=27002
host="127.0.0.1"
tmp=`find ${sync} | grep upstart_template$`
cat ${tmp} | sed -e "s|\(.*\)__PATH_TO_LOCKER__\(.*\)|\1${p}\2|g" > /etc/init/locker.conf
sudo initctl reload-configuration
sudo service locker restart
