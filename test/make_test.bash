#!/bin/bash

# FIRST get Alchemy's Lua Client
git clone https://JakSprats@github.com/JakSprats/redis-lua.git

# then install luasockets
wget http://luaforge.net/frs/download.php/2664/luasocket-2.0.2.tar.gz
tar xvfz luasocket-2.0.2.tar.gz
(cd luasocket-2.0.2;
  make install
}
