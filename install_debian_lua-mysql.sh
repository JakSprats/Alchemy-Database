#/bin/bash

#install mysql client lib and headers
sudo apt-get install libmysqlclient-dev

#download luasql and make install
wget http://luaforge.net/frs/download.php/2686/luasql-2.1.1.tar.gz
tar xvfz luasql-2.1.1.tar.gz
#overwrite default config w/ debian config
cp lua-5.1.4/luasql-debian.config luasql-2.1.1/config
(cd luasql-2.1.1/
  make
  sudo make install
)
