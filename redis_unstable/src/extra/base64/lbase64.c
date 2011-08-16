/*
* lbase64.c
* base64 encoding and decoding for Lua 5.1
* Luiz Henrique de Figueiredo <lhf@tecgraf.puc-rio.br>
* 23 Mar 2010 22:22:38
* This code is hereby placed in the public domain.
*/

#include <string.h>

#include "lua.h"
#include "lauxlib.h"

#define MYNAME		"base64"
#define MYVERSION	MYNAME " library for " LUA_VERSION " / Mar 2010"

#define uint unsigned int

static const char code[]=
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void encode(luaL_Buffer *b, uint c1, uint c2, uint c3, int n)
{
 unsigned long tuple=c3+256UL*(c2+256UL*c1);
 int i;
 char s[4];
 for (i=0; i<4; i++) {
  s[3-i] = code[tuple % 64];
  tuple /= 64;
 }
 for (i=n+1; i<4; i++) s[i]='=';
 luaL_addlstring(b,s,4);
}

static int Lencode(lua_State *L)		/** encode(s) */
{
 size_t l;
 const unsigned char *s=(const unsigned char*)luaL_checklstring(L,1,&l);
 luaL_Buffer b;
 int n;
 luaL_buffinit(L,&b);
 for (n=l/3; n--; s+=3) encode(&b,s[0],s[1],s[2],3);
 switch (l%3)
 {
  case 1: encode(&b,s[0],0,0,1);		break;
  case 2: encode(&b,s[0],s[1],0,2);		break;
 }
 luaL_pushresult(&b);
 return 1;
}

static void decode(luaL_Buffer *b, int c1, int c2, int c3, int c4, int n)
{
 unsigned long tuple=c4+64L*(c3+64L*(c2+64L*c1));
 char s[3];
 switch (--n)
 {
  case 3: s[2]=tuple;
  case 2: s[1]=tuple >> 8;
  case 1: s[0]=tuple >> 16;
 }
 luaL_addlstring(b,s,n);
}

static int Ldecode(lua_State *L)		/** decode(s) */
{
 size_t l;
 const char *s=luaL_checklstring(L,1,&l);
 luaL_Buffer b;
 int n=0;
 char t[4];
 luaL_buffinit(L,&b);
 for (;;)
 {
  int c=*s++;
  switch (c)
  {
   const char *p;
   default:
    p=strchr(code,c); if (p==NULL) return 0;
    t[n++]= p-code;
    if (n==4)
    {
     decode(&b,t[0],t[1],t[2],t[3],4);
     n=0;
    }
    break;
   case '=':
    switch (n)
    {
     case 1: decode(&b,t[0],0,0,0,1);		break;
     case 2: decode(&b,t[0],t[1],0,0,2);	break;
     case 3: decode(&b,t[0],t[1],t[2],0,3);	break;
    }
   case 0:
    luaL_pushresult(&b);
    return 1;
   case '\n': case '\r': case '\t': case ' ': case '\f': case '\b':
    break;
  }
 }
 return 0;
}

static const luaL_Reg R[] =
{
	{ "encode",	Lencode	},
	{ "decode",	Ldecode	},
	{ NULL,		NULL	}
};

LUALIB_API int luaopen_base64(lua_State *L)
{
 luaL_register(L,MYNAME,R);
 lua_pushliteral(L,"version");			/** version */
 lua_pushliteral(L,MYVERSION);
 lua_settable(L,-3);
 return 1;
}
