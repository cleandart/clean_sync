#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
p="${DIR}/../bin/mongo_locker.dart"
cat upstart_template | sed -e "s|__PATH_TO_LOCKER__|${p}|g" > /etc/init/locker.conf
sudo initctl reload-configuration
sudo service locker restart
