--- LuaStash Module
-- This module provides functionality to process data through a pipeline of generators, filters,and outputs.
-- It includes logging and error handling mechanisms.
-- @module luastash

local pipeflow = require("pipeflow")
local dkjson = require("dkjson")

-- Logger
local logger_map_level_name = {
	debug = 0,
	info = 1,
	warn = 2,
	error = 3,
}

--- Logger class for logging messages with different levels (debug, info, warn, error). It supports enabling/disabling logging and creating child loggers with specific names and levels.
-- @type Logger
local Logger = {}

--- Create a new Logger instance.
-- @param options A table of options to configure the logger.
-- @param[opt=''] options.name The name of the logger.
-- @param[opt='info'] options.level The logging level (debug, info, warn, error).
-- @param[opt=true] options.enabled Whether the logger is enabled.
-- @treturn Logger A new `Logger` instance configured with the provided options.
function Logger:new(options)
	local instance = {
		level = "info",
		name = options and options.name or "",
		enabled = options and options.enabled or true,
	}

	setmetatable(instance, { __index = Logger })

	if options and options.level then
		instance:set_level(options.level)
	end

	local function createLoggerLevel(label)
		return function(text, ...)
			if instance.enabled and logger_map_level_name[label] >= logger_map_level_name[instance.level] then
				local arg = { ... }
				print(
					string.format(
						"%s %s[%s]: %s",
						os.date("!%c"),
						instance.name and string.format("{%s} ", instance.name) or "",
						label,
						#arg > 0 and string.format(text, ...) or text
					)
				)
			end
		end
	end

	for _, label in ipairs({ "debug", "info", "warn", "error" }) do
		instance[label] = createLoggerLevel(label)
	end

	return instance
end

--- Set the logging level for the logger instance. The level can be one of "debug", "info", "warn", or "error". If an invalid level is provided, an error will be raised.
-- @param level The logging level to set (debug, info, warn, error).
function Logger:set_level(level)
	if logger_map_level_name[level] == nil then
		error(string.format("Level is not allowed: %s", tostring(level)))
	end
	self.level = level
end

--- Disable logging for this logger instance. When disabled, no log messages will be printed regardless of the logging level.
-- @return nil
function Logger:disable()
	self.enabled = false
end

--- Enable logging for this logger instance. When enabled, log messages will be printed according to the configured logging level.
-- @return nil
function Logger:enable()
	self.enabled = true
end

--- Create a child logger with specific options. The child logger will inherit the logging level and enabled state from the parent logger unless overridden in the options. The name of the child logger will be a combination of the parent logger's name and the provided name in the options.
-- @param options A table of options to configure the child logger.
-- @param[opt=''] options.name The name of the child logger, which will be combined with the parent logger's name.
-- @param[opt=''] options.level The logging level for the child logger. If not provided, it will inherit the parent's logging level.
-- @param[opt=true] options.enabled Whether the child logger is enabled. If not provided, it will inherit the parent's enabled state.
-- @treturn Logger A new `Logger` instance configured with the provided options and inheriting from the parent logger.
function Logger:get_logger(options)
	local new_options = {
		level = (options and options.level ~= nil and options.level) or self.level,
		enabled = options and options.enabled or self.enabled,
	}

	if options and options.name then
		new_options.name = string.format("%s:%s", self.name, options.name)
	end

	return Logger:new(new_options)
end

--- Event class representing a data event in the pipeline. It provides methods to get and set fields using Logstash-style paths, manage metadata, clone events, and serialize to JSON.
-- @type Event
local Event = {}
Event.__index = Event

-- Utility: split "[a][b][c]" into {"a","b","c"}
local function parse_path(path)
    local parts = {}
    for key in path:gmatch("%[([^%]]+)%]") do
        parts[#parts + 1] = key
    end
    return parts
end

-- Utility: deep clone
local function deep_copy(tbl)
    if type(tbl) ~= "table" then return tbl end
    local copy = {}
    for k, v in pairs(tbl) do
        copy[k] = deep_copy(v)
    end
    return copy
end

--- Create a new Event instance.
-- @usage
-- local event = Event:new({foo = "bar"})
-- @tparam[opt=nil] data data Initial event data, defaults to an empty table if not provided.
-- @treturn Event A new Event instance with the provided data and default metadata.
function Event:new(data)
    local instance = {
        data = data or {},
        metadata = {},
    }

    -- Logstash defaults
    instance.metadata["@timestamp"] = type(instance.data) == "table" and instance.data["@timestamp"] or os.date("!%Y-%m-%dT%H:%M:%SZ")
    instance.metadata["@version"] = type(instance.data) == "table" and instance.data["@version"] or "1"

    return setmetatable(instance, self)
end

--- Get field using "[foo][bar]" syntax. If the path does not exist, it returns nil.
-- @usage
-- local value = event:get("[foo][bar]")
-- @tparam string path The field path in Logstash-style syntax (e.g., "[foo][bar]").
-- @return The value at the specified path, or nil if the path does not exist.
function Event:get(path)
    local parts = parse_path(path)
    local ref = self.data

    for _, key in ipairs(parts) do
        if type(ref) ~= "table" then return nil end
        ref = ref[key]
        if ref == nil then return nil end
    end

    return ref
end

--- Set field using "[foo][bar]" syntax. If intermediate tables do not exist, they will be created.
-- @usage
-- event:set("[foo][bar]", "value")
-- @tparam string path The field path in Logstash-style syntax (e.g., "[foo][bar]").
-- @param value The value to set at the specified path. If intermediate tables do not exist, they will be created.
function Event:set(path, value)
    local parts = parse_path(path)
    local ref = self.data

    for i = 1, #parts - 1 do
        local key = parts[i]
        if type(ref[key]) ~= "table" then
            ref[key] = {}
        end
        ref = ref[key]
    end

    ref[parts[#parts]] = value
end


--- Get metadata field.
-- @usage
-- local value = event:get_metadata("key")
-- @tparam string key The metadata key
-- @return any The metadata value
function Event:get_metadata(key)
    return self.metadata[key]
end

--- Set metadata field.
-- @usage
-- event:set_metadata("key", "value")
-- @tparam string key The metadata key
-- @tparam any value The metadata value
-- @return nil
function Event:set_metadata(key, value)
    self.metadata[key] = value
end

--- Clone event.
-- @usage
-- local event2 = event:clone()
-- @treturn Event A new event with the same data and metadata
function Event:clone()
    local copy = Event:new(deep_copy(self.data))
    copy.metadata = deep_copy(self.metadata)
    return copy
end

--- Serialize event to JSON.
-- @usage
-- local json = event:to_json()
-- @treturn string JSON representation of the event data
function Event:to_json()
    return dkjson.encode(self.data)
end

--- Event metadata fields. These fields provide additional information about the event and are used for processing and routing within the stash pipeline. The metadata includes standard Logstash fields such as `@timestamp` and `@version`, as well as custom fields like `source` and `source_tag` that indicate the origin of the event.
-- @table metadata
-- @field @timestamp The timestamp of the event, defaulting to the current time in ISO 8601 format if not provided in the initial data.
-- @field @version The version of the event, defaulting to "1" if not provided in the initial data.
-- @field source The input type of the event.
-- @field source_tag The tag of the input if provided in the configuration.

--- Event data fields. These fields contain the actual data of the event that is processed through the stash pipeline. The data can be structured in a nested manner, and fields can be accessed and modified using Logstash-style paths (e.g., "[foo][bar]").
-- @field data The main data table of the event, which can contain any structured data relevant to the event being processed.

--- Run Stash.
-- @section	RunStash

--- Read and parse JSON configuration from a file. This function opens the specified file, reads its contents, and decodes the JSON data into a Lua table. If the file cannot be opened or if the JSON is invalid, an error will be raised.
-- @local
-- @function get_config_from_file
-- @usage
-- local config = get_config_from_file("config.json")
-- @param filename The path to the JSON configuration file.
-- @treturn table The parsed configuration as a Lua table.
local function get_config_from_file(filename)
	local file = io.open(filename, "r") -- Open file in read mode
	if not file then
		return nil, "Error: Could not open file"
	end

	local content = file:read("*a") -- Read entire file contents
	file:close() -- Close the file

	-- Parse JSON to Lua table
	local success, data = pcall(dkjson.decode, content)
	if not success then
		error("Error: Invalid JSON format")
	end

	return data
end

--- Set up input generators based on the provided configuration. This function iterates through the list of input configurations, checks if a corresponding generator function exists in the `inputs` table of the `processors`, and initializes each generator with its options, context, and utilities. The initialized generators are stored in a list and returned for further processing.
-- @param config_inputs A list of input configurations, where each configuration should include a `type` field that corresponds to a processor function in the `inputs` table of the `StashProcessors`.
-- @param inputs A table of input processor functions defined in the `StashProcessors`.
-- @param ctx Context object passed to the generator functions.
-- @param utils Utility functions and logger passed to the generator functions.
-- @treturn table A list of initialized input generators, each containing its type, tag, options, and the next function to call for generating data.
local function setup_inputs(config_inputs, inputs, ctx, utils)
	local generators = {}

	for i, v in ipairs(config_inputs) do
		if inputs[v.type] then
			local generator = {
				type = v.type,
				tag = v.tag,
				options = v.options,
				next = inputs[v.type](v.options, ctx, utils),
			}
			table.insert(generators, generator)
		else
			error(string.format("Input type '%s' is not defined in processors", v.type))
		end
	end
	return generators
end

--- Run the stash pipeline. This function processes data through a series of generators, filters, and outputs defined in the configuration. It handles logging and error management throughout the processing. The function continues to call the generators until all generators have been exhausted (i.e., they return nil), at which point it finishes the stash processing.
-- @param config Pipeline configuration table or file path. If a string is provided, it will be treated as a file path to a JSON configuration file, which will be read and parsed into a Lua table.
-- @param processors A table containing the input, filter, and output processor functions defined in the `StashProcessors`.
-- @param[opt=nil] ctx Context object passed to the processor functions.
-- @param[opt=nil] utils Utility functions and logger passed to the processor functions. If not provided, a default logger will be used.
-- @return nil
local function run_stash(config, processors, ctx, utils)
	local logger = utils and utils.logger or Logger:new({ name = "luastash", level = "debug" })

	logger.debug("Running stash")
	local _config = config
	if type(_config) == "string" then
		local _configfile = _config
		logger.debug("Reading file: %s", _configfile)
		_config = get_config_from_file(_config)
		logger.debug("Readed file: %s, content: %s", _configfile, dkjson.encode(_config))
	end

	if _config.inputs == nil or processors.inputs == nil then
		local message = "Inputs are not defined"
		logger.error(message)
		error(message)
	end

	if _config.outputs == nil or processors.outputs == nil then
		local message = "Outputs are not defined"
		logger.error(message)
		error(message)
	end

	local pipeflow_options = {
		eval_accessor_data = 'event'
	}

	logger.debug("Setup input generators")
	local generators = setup_inputs(_config.inputs, processors.inputs, ctx, utils)

	logger.debug("Input generators initialized [%s]", #generators)

	local generator_index = 1
	while #generators > 0 do
		local success, err = pcall(function()
			local increment_generator_index = true
			local generator = generators[generator_index]
			local logger_generator = logger:get_logger({
				name = string.format("input[type=%s]", generator.type)
					.. (generator.tag and string.format("[tag=%s]", generator.tag) or ""),
			})

			logger_generator.debug("Calling generator [%s] of [%s]", generator_index, #generators)
			logger_generator.debug("Getting next value from generator [%s] of [%s]", generator_index, #generators)
			local gen_continue, message = generator.next(generator.options, ctx, utils)
			logger_generator.debug(
				"Next value from generator [%s] of [%s]: %s",
				generator_index,
				#generators,
				tostring(message)
			)

			if not gen_continue then
				logger_generator.debug(
					"Removing generator [%s] of [%s] due to returned nil",
					generator_index,
					#generators
				)
				table.remove(generators, generator_index)
				logger_generator.debug(
					"Generator [%s] was removed. Remaining generators [%s]",
					generator_index,
					#generators
				)
				increment_generator_index = false
			else
				if message == nil then
					-- Do nothing keeping the input generator
					logger_generator.debug("Skip input due to nil value")
				else
					local event = Event:new(message)
					event:set_metadata('source', generator.type)
					if generator.tag then
						event:set_metadata('source_tag', generator.tag)
					end
					logger_generator.debug("Processing data")
					if _config.filters then
						if processors.filters == nil then
							error("Processors filters are not defined")
						end
						logger_generator.debug("Processing filters")
						event = pipeflow(
							{ name = "filters", processors = _config.filters, options = pipeflow_options },
							processors.filters,
							event,
							ctx,
							{ logger = logger_generator }
						)
						logger_generator.debug("Processed filters")
					end

					-- Avoid run the outputs pipeline if the transformed data is nil
					if event ~= nil then
						logger_generator.debug("Processing outputs")
						pipeflow(
							{ name = "outputs", processors = _config.outputs, options = pipeflow_options },
							processors.outputs,
							event,
							ctx,
							{ logger = logger_generator }
						)
						logger_generator.debug("Processed outputs")
					else
						logger_generator.debug("Skipped outputs")
					end
					logger_generator.debug("Data was processed")
				end
			end
			return { increment_generator_index = increment_generator_index }
		end)

		if not success then
			-- logger.error("Error processing: %s", err)
			logger.debug("Error stack trace: %s", debug.traceback(err))
		end

		if type(err) == "table" and err.increment_generator_index == true then
			generator_index = generator_index + 1
			logger.debug(string.format("Incremented generator index [%s] of [%s]", generator_index, #generators))
		end

		if generator_index > #generators then
			logger.debug(string.format("Reset to the first generator due to generator index [%s] is greater than generators count [%s]", generator_index, #generators))
			generator_index = 1
		end
	end

	logger.debug("Stash finished")
end

--- Pipeline configuration table. 
--- @table StashConfig
--- @field inputs List of input processor configurations. Each configuration should include a `type` field that corresponds to a processor function in the `inputs` table of the `StashProcessors`.
--- @field filters List of filter processor configurations. Each configuration should include a `type` field that corresponds to a processor function in the `filters` table of the `StashProcessors`.
--- @field outputs List of output processor configurations. Each configuration should include a `type` field that corresponds to a processor function in the `outputs` table of the `StashProcessors`.
--- @usage
--- local config = {
---     inputs = {
---         { type = "input_type_1", options = { ... } },
---         { type = "input_type_2", options = { ... } },
---     },
---     filters = {
---         { type = "filter_type_1", options = { ... } },
---         { type = "filter_type_2", options = { ... } },
---     },
---     outputs = {
---         { type = "output_type_1", options = { ... } },
---         { type = "output_type_2", options = { ... } },
---     },
---     options = { ... },
--- }

--- Define the inputs, filters and outputs processors.
-- This table should contain functions for each processor type used in the stash pipeline.
-- @table StashProcessors
-- @field inputs Defines input processor functions.
-- @field filters Defines filter processor functions.
-- @field outputs Defines output processor functions.

--- Module export
-- @section ModuleExport

local M = {}
--- Logger instance.
-- @field Logger Provide a logger constructor.
M.Logger = Logger

--- Event instance.
-- @field Event Provide an event constructor.
M.Event = Event

--- Version of the LuaStash module.
-- @field _VERSION Define the module version.
M._VERSION = "v0.2.0"

--- Runs the stash pipeline.
-- This function processes data through a series of generators, filters and outputs.
-- @function call
-- @usage
-- local luastash = require("luastash")
-- luastash(config, processors, ctx, utils)
-- @tparam StashPipelineConfig|string config Pipeline configuration table or file path
-- @tparam StashProcessors processors Inputs, filters and outputs processors
-- @tparam[opt=nil] any context Context object passed to processors
-- @tparam[opt=nil] table utils Utility functions and logger
-- @param[opt=nil] utils.logger Logger instance for logging within processors
-- @return nil

return setmetatable({}, {
	__call = function(t, ...)
		return run_stash(...)
	end,
	__index = M,
})
