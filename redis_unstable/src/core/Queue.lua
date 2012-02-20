--[[
-- Queue.lua: Simple queue objects
-- Author: Luis Carvalho
-- Lua Programming Gems: "Building Data Structures and Iterators in Lua"
--]]

local assert, getfenv, setmetatable = assert, getfenv, setmetatable

module(...)
local modenv = getfenv() -- module environment

function new ()
  return setmetatable({first = 1, last = 0}, {__index = modenv})
end

function insert (Q, v)
  assert(v ~= nil, "cannot insert nil")
  local last = Q.last + 1
  Q[last] = v
  Q.last = last
end

function retrieve (Q)
  local first = Q.first
  assert(Q.last >= first, "cannot retrieve from empty queue")
  local v = Q[first]
  Q[first] = nil -- allow GC
  Q.first = first + 1
  return v
end

function retrieveFromEnd (Q)
  local last  = Q.last
  assert(last >= Q.first, "cannot retrieveFromEnd from empty queue")
  local v = Q[last]
  Q[last] = nil -- allow GC
  Q.last = last - 1
  return v
end

function isempty (Q) return Q.last < Q.first end

