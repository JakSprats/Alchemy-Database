
#include "alchemy.h"

#include "redis.h"
#include "common.h"
#include "embed.h"

// PROTOTYPES
int luaRedisCommand(lua_State *lua);              // from scripting.c
int luaLogCommand(lua_State *lua);                // from scripting.c

static const luaL_reg alchemy_f[] = {
  {"call", luaRedisCommand},
  {"log",  luaLogCommand},
  {NULL, NULL}
};

LUALIB_API int luaopen_alchemy(lua_State *L) {
    initEmbeddedAlchemy();
    lua_newtable (L);
    luaL_register(L, NULL, alchemy_f);
    return 1;
}

