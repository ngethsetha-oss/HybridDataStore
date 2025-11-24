-- HybridDataStore.lua
-- Robust Hybrid profile system for Roblox DataStore
-- Features:
--   - In-memory caching with dirty keys
--   - Delta saving for efficiency
--   - Template reconciliation
--   - Signals: OnLoaded, OnSaving, OnUpdate, OnError
--   - Safe EndSession with HasMadeChangesAfterSave
--   - Atomic UpdateAsync wrapper
--   - Optional compressor support
--   - Compatible with inventory systems

local DataStoreService = game:GetService("DataStoreService")
local Signal = require(game.ServerScriptService.Packages.Signal)
local HttpService = game:GetService("HttpService")

local HybridStore = {}
HybridStore.__index = HybridStore

-- -----------------------
-- Utilities
-- -----------------------
local function deepCopy(tbl)
	if tbl == nil then return nil end
	local new = {}
	for k, v in pairs(tbl) do
		if type(v) == "table" then
			new[k] = deepCopy(v)
		else
			new[k] = v
		end
	end
	return new
end

local function applyTemplate(data, template)
	data = data or {}
	for k, v in pairs(template or {}) do
		if data[k] == nil then
			if type(v) == "table" then
				data[k] = deepCopy(v)
			else
				data[k] = v
			end
		end
	end
	return data
end

-- Internal helper: attempt UpdateAsync with retries and exponential backoff
local function tryUpdateWithRetries(ds, key, updateFn, maxRetries, backoffBase)
	local attempt = 0
	while true do
		local ok, res = pcall(function()
			return ds:UpdateAsync(key, updateFn)
		end)
		if ok then
			return true, res
		end
		attempt = attempt + 1
		if attempt >= (maxRetries or 1) then
			return false, res
		end
		local waitTime = (backoffBase or 0.5) * (2 ^ (attempt - 1))
		task.wait(waitTime)
	end
end

-- -----------------------
-- Hybrid profile class
-- -----------------------
local Hybrid = {}
Hybrid.__index = Hybrid

-- opts: retryCount, backoffBase, compressor
function Hybrid.new(datastore, key, template, opts)
	opts = opts or {}
	local compressor = opts.compressor or {
		compress = function(v) return v end,
		decompress = function(v) return v end,
	}
	return setmetatable({
		DataStore = datastore,
		Key = key,
		Template = template or {},
		Data = nil,
		_dirty = {},
		_ended = false,
		_retryCount = opts.retryCount or 5,
		_backoffBase = opts.backoffBase or 0.5,
		_compressor = compressor,
		HasMadeChangesAfterSave = false,
		-- Signals
		OnLoaded = Signal.New(),
		OnSaving = Signal.New(),
		OnError = Signal.New(),
		OnUpdate = Signal.New(),
	}, Hybrid)
end

-- Load: safe GetAsync, decompress, apply template, clear dirty
function Hybrid:Load()
	local success, raw = pcall(function()
		return self.DataStore:GetAsync(self.Key)
	end)
	if not success then
		self.OnError:Fire("LoadFailed", raw)
		warn("[Hybrid] Load failed for key:", self.Key, raw)
	end

	local decompressed = {}
	if type(raw) == "table" then
		for k, v in pairs(raw) do
			local ok, dec = pcall(self._compressor.decompress, v)
			if ok then
				decompressed[k] = dec
			else
				decompressed[k] = v
			end
		end
	end

	self.Data = applyTemplate(decompressed, self.Template)
	self._dirty = {}
	self.HasMadeChangesAfterSave = false
	self.OnLoaded:Fire(self.Data)
	return self.Data
end

-- Save: delta save only dirty keys or given key list
function Hybrid:Save(keys)
	if self._ended then return true end
	self.OnSaving:Fire(self.Data)

	local keysToSave = {}
	if type(keys) == "table" and #keys > 0 then
		for _, k in ipairs(keys) do table.insert(keysToSave, k) end
	else
		for k in pairs(self._dirty) do table.insert(keysToSave, k) end
	end

	if #keysToSave == 0 then
		return true
	end

	local snapshot = {}
	for _, k in ipairs(keysToSave) do
		local ok, comp = pcall(self._compressor.compress, self.Data[k])
		if ok then
			snapshot[k] = comp
		else
			snapshot[k] = self.Data[k]
			warn("[Hybrid] compressor.compress failed for key", k, ":", comp)
		end
	end

	local success, err = tryUpdateWithRetries(self.DataStore, self.Key, function(old)
		old = old or {}
		for k, v in pairs(snapshot) do
			old[k] = v
		end
		return old
	end, self._retryCount, self._backoffBase)

	if not success then
		self.OnError:Fire("SaveFailed", err)
		warn("[Hybrid] Save failed for key:", self.Key, err)
		return false, err
	end

	for _, k in ipairs(keysToSave) do
		self._dirty[k] = nil
	end
	self.HasMadeChangesAfterSave = false
	return true
end

-- Set: update memory, mark dirty, fire OnUpdate
function Hybrid:Set(key, value)
	if self._ended then return end
	local oldValue = self.Data and self.Data[key] or nil
	local oldCopy = (type(oldValue) == "table") and deepCopy(oldValue) or oldValue
	local newCopy = (type(value) == "table") and deepCopy(value) or value

	if not self.Data then self.Data = {} end
	self.Data[key] = value
	self._dirty[key] = true
	self.HasMadeChangesAfterSave = true

	self.OnUpdate:Fire(key, oldCopy, newCopy)
end

-- Update: atomic UpdateAsync wrapper
function Hybrid:Update(key, callback)
	if self._ended then return nil, "ended" end
	if type(callback) ~= "function" then error("[Hybrid] Update callback must be a function") end

	local oldVal, newVal
	local success, err = tryUpdateWithRetries(self.DataStore, self.Key, function(old)
		old = old or {}
		local rawOld = old[key]
		local ok, dec = pcall(self._compressor.decompress, rawOld)
		local current = ok and dec or rawOld

		oldVal = current
		newVal = callback(current)

		local ok2, comp = pcall(self._compressor.compress, newVal)
		if ok2 then
			old[key] = comp
		else
			old[key] = newVal
			warn("[Hybrid] compressor.compress failed during Update for key", key, comp)
		end
		return old
	end, self._retryCount, self._backoffBase)

	if not success then
		self.OnError:Fire("UpdateFailed", err)
		warn("[Hybrid] Update failed for key:", self.Key, err)
		return nil, err
	end

	if not self.Data then self.Data = {} end
	self.Data[key] = newVal
	self._dirty[key] = nil
	self.HasMadeChangesAfterSave = false

	local oldCopy = (type(oldVal) == "table") and deepCopy(oldVal) or oldVal
	local newCopy = (type(newVal) == "table") and deepCopy(newVal) or newVal
	self.OnUpdate:Fire(key, oldCopy, newCopy)

	return newVal
end

-- Get in-memory table
function Hybrid:Get()
	return self.Data
end

-- Reconcile: ensure template keys exist
function Hybrid:Reconcile()
	if not self.Data then self.Data = {} end
	for k, v in pairs(self.Template or {}) do
		if self.Data[k] == nil then
			self.Data[k] = (type(v) == "table") and deepCopy(v) or v
			self._dirty[k] = true
			self.HasMadeChangesAfterSave = true
		end
	end
end

-- EndSession: safe final save, cache cleanup
function Hybrid:EndSession()
	if self._ended then return end
	self._ended = true

	if self.HasMadeChangesAfterSave then
		local ok = self:Save()
		if not ok then
			warn("[HybridStore] Save failed during EndSession, retrying…")
			task.wait(1)
			self:Save()
		end
	end

	-- Clean memory
	self.Data = nil
	self._dirty = {}
	self.HasMadeChangesAfterSave = false
end

-- Set compressor
function Hybrid:SetCompressor(compressor)
	if type(compressor) ~= "table" or type(compressor.compress) ~= "function" or type(compressor.decompress) ~= "function" then
		error("[Hybrid] Compressor must be a table with compress/decompress functions")
	end
	self._compressor = compressor
end

-- -----------------------
-- HybridStore (factory)
-- -----------------------
function HybridStore.new(masterKey, template)
	return setmetatable({
		MasterKey = masterKey,
		Template = template or {},
	}, HybridStore)
end

function HybridStore:StartSessionAsync(playerOrKey, opts)
	local ds = DataStoreService:GetDataStore(self.MasterKey)
	local key
	if typeof(playerOrKey) == "Instance" and playerOrKey:IsA("Player") then
		key = tostring(playerOrKey.UserId) .. "_" .. tostring(self.MasterKey)
	else
		key = tostring(playerOrKey) .. "_" .. tostring(self.MasterKey)
	end
	return Hybrid.new(ds, key, self.Template, opts)
end

return HybridStore