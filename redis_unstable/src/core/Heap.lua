--[[
-- Heap.lua: Priority heap objects
-- Author: Luis Carvalho
-- Lua Programming Gems: "Building Data Structures and Iterators in Lua"
--]]

local assert, getfenv, setmetatable = assert, getfenv, setmetatable

module(...)
local modenv = getfenv()

function new (gv_func)
  return setmetatable({record = {}, key = {}, ref = {}, gv = gv_func},
                      {__index = modenv})
end

local function parent (n)
  return (n - n % 2) / 2 -- floor(n / 2)
end

function update (H, k, v)
  local record, key, ref = H.record, H.key, H.ref
  local i = ref[k]
  local p = parent(i)
  while i > 1 and H.gv(record[p]) > H.gv(v) do -- climb tree?
    -- exchange nodes
    record[i], key[i], ref[key[p]] = record[p], key[p], i
    i, p = p, parent(p)
  end
  record[i], key[i], ref[k] = v, k, i -- update
end

function insert (H, k, v)
  local ref = H.ref
  assert(ref[k] == nil, "key already in heap")
  ref[k] = #H.record + 1 -- insert reference
  update(H, k, v) -- insert record
end

local function heapify (H, record, root, key, ref, n)
  local left, right = 2 * root, 2 * root + 1 -- children
  local p, l, r = record[root], record[left], record[right]
  -- find min := argmin_{root, left, right}(record)
  local min, m = root, p
  if (left  <= n and H.gv(l) < H.gv(m)) then min, m = left,  l end
  if (right <= n and H.gv(r) < H.gv(m)) then min, m = right, r end
  if min ~= root then -- recurse to fix subtree?
    record[root], record[min] = m, p -- exchange records...
    key[root], key[min] = key[min], key[root] -- and keys
    if ref ~= nil then -- fix refs?
      ref[key[root]], ref[key[min]] = root, min
    end
    return heapify(H, record, min, key, ref, n)
  end
end

function retrieve (H)
  local record, key, ref = H.record, H.key, H.ref
  assert(record[1] ~= nil, "cannot retrieve from empty heap")
  local n = #record -- heap size
  local minr, mink = record[1], key[1]
  record[1], key[1], ref[key[n]] = record[n], key[n], 1 -- leaf to root
  record[n], key[n], ref[mink] = nil, nil, nil -- remove leaf
  heapify(H, record, 1, key, ref, n - 1) -- fix heap
  return mink, minr
end

function isempty (H) return H.record[1] == nil end

function heapsort (H, t)
  local n = #t -- heap size
  local k = {} -- key: position in t
  for i = 1, n do k[i] = i end
  for i = parent(n), 1, -1 do -- build heap
    heapify(H, t, i, k, nil, n)
  end
  for i = n, 2, -1 do -- sort in-place
    k[1], k[i] = k[i], k[1] -- exchange keys...
    t[1], t[i] = t[i], t[1] -- ...and records
    heapify(H, t, 1, k, nil, i - 1)
  end
  return k
end

