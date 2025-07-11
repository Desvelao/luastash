--- LuaStash Module
-- This module provides functionality to process data through a pipeline of generators and outputs.
-- It includes logging and error handling mechanisms.
-- @module luastash

local pipeflow = require("pipeflow")
local dkjson = require("dkjson")

-- Logger
local logger_map_level_name = {
    debug=0,
    info=1,
    warn=2,
    error=3
}

local Logger = {}

function Logger:new(options)
    local instance = {
        level = "info",
        name = options and options.name or "",
        enabled = options and options.enabled or true
    }

    setmetatable(instance, {__index = Logger})

    if options and options.level then
        instance:set_level(options.level)
    end

    local function createLoggerLevel(label)
        return function(text, ...)
            if instance.enabled and logger_map_level_name[label] >= logger_map_level_name[instance.level] then
                local arg = {...}
                print(string.format(
                    "%s %s[%s]: %s",
                    os.date("!%c"),
                    instance.name and string.format("{%s} ", instance.name) or "",
                    label,
                    #arg > 0 and string.format(text, ...) or text
                ))
            end
        end
    end

    for _, label in ipairs({'debug', 'info', 'warn', 'error'}) do
        instance[label] = createLoggerLevel(label)
    end

    return instance
end

function Logger:set_level(level)
    if logger_map_level_name[self.level] == nil then
        error(string.format("Level is not allowed: %s", tostring(level)))
    end
    self.level = level
end

function Logger:disable(level)
    self.enabled = false
end

function Logger:enable(level)
    self.enabled = false
end

function Logger:get_logger(options)
    local new_options = {
        level = (options and options.level ~= nil and options.level) or self.level,
        enabled = options and options.enabled or self.enabled
    }

    if options and options.name then
        new_options.name = string.format("%s:%s", self.name, options.name)
    end

    return Logger:new(new_options)
end

local LoggerMain = Logger:new({name='luastash', level = 'debug'})

-- Core

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

local function setup_inputs(config_inputs, inputs, ctx, utils)
	local generators = {}
	for i, v in ipairs(config_inputs) do
		local generator
		if inputs[v.type] then
			local generator = {
				type = v.type,
				tag = v.tag,
				options = v.options,
				next = inputs[v.type](v.options, ctx, utils),
			}
			table.insert(generators, generator)
		end
	end
	return generators
end

local INPUT_CONTINUE_SIGNAL = "__INPUT_CONTINUE_SIGNAL__"

local function run_stash(config, processors, ctx, utils)
	local logger = utils and utils.logger or LoggerMain

	logger.debug("Running stash")
	local _config = config
	if type(_config) == "string" then
		local _configfile = _config;
		logger.debug("Reading file: %s", _configfile)
		_config = get_config_from_file(_config)
		logger.debug("Readed file: %s, content: %s", _configfile, dkjson.encode(_config))
	end

	if _config.outputs == nil or processors.outputs == nil then
		local message = "Outputs are not defined"
		logger.error(message)
		error(message)
	end

	logger.debug("Setup input generators")
	local generators = setup_inputs(_config.inputs, processors.inputs, ctx, utils)

	logger.debug("Input generators initialized [%s]", #generators)

	local generator_index = 1
	while #generators > 0 do
		local generator = generators[generator_index]
		local logger_generator = logger:get_logger({
			name = string.format("input[type=%s]", generator.type) .. (generator.tag and string.format("[tag=%s]", step.tag) or "")
		})

		logger_generator.debug("Calling generator [%s] of [%s]", generator_index, #generators)
		logger_generator.debug("Getting next value from generator [%s] of [%s]", generator_index, #generators)
		local data = generator.next(generator.options, ctx, utils)
		logger_generator.debug(
			"Next value from generator [%s] of [%s]: %s",
			generator_index,
			#generators,
			tostring(data)
		)

		if data == nil then
			logger_generator.debug("Removing generator [%s] of [%s] due to returned nil", generator_index, #generators)
			table.remove(generators, generator_index)
			logger_generator.debug("Generator [%s] was removed. Remaining generators [%s]", generator_index, #generators)
		else
			if data == INPUT_CONTINUE_SIGNAL then
				-- Do nothing keeping the input generator
                logger_generator.debug("Skip input due to continue signal")
			else
				logger_generator.debug("Processing data")
				if _config.filters then
					if processors.filters == nil then
						error("Processors filters are not defined")
					end
					logger_generator.debug("Processing filters")
					data = pipeflow({ name = "filters", processors = _config.filters }, processors.filters, data, ctx, {logger=logger_generator})
					logger_generator.debug("Processed filters")
				end
			
				-- Avoid run the outputs pipeline if the transformed data is nil
				if data ~= nil then
					logger_generator.debug("Processing outputs")
					data = pipeflow({ name = "outputs", processors = _config.outputs }, processors.outputs, data, ctx, {logger=logger_generator})
					logger_generator.debug("Processed outputs")
				else
					logger_generator.debug("Skipped outputs")
				end

                logger_generator.debug("Data was processed")
			end
            logger.debug("Pass to following generator")
			generator_index = generator_index + 1
			if generator_index > #generators then
				generator_index = 1
			end
		end
	end

	logger.debug("Stash finished")
end

--- Define the inputs, filters and outputs processors.
-- This table should contain functions for each processor type used in the stash pipeline.
-- @table StashProcessors
-- @field inputs Defines input processor functions.
-- @field filters Defines filter processor functions.
-- @field outputs Defines output processor functions.

--- A metatable that provides callable and indexable behavior.
-- This table allows calling `run_stash` directly and provides a method to create a logger.
-- @table Stash
-- @field __call Calls the `run_stash` function with the provided arguments.
-- @field __index Contains utility methods, such as `create_logger` and `INPUT_CONTINUE_SIGNAL`.

--- Runs the stash pipeline.
-- This function processes data through a series of generators, filters and outputs.
-- @function __call
-- @usage
-- local luastash = require("luastash")
-- luastash(config, processors, ctx, utils)
-- @tparam table config Pipeline configuration or file path
-- @tparam StashProcessors processors Inputs, filters and outputs processors
-- @tparam table ctx Context object passed to processors
-- @tparam table utils Utility functions and logger
-- @return nil

--- Creates a new logger instance.
-- @function create_logger
-- @usage
-- local luastash = require("luastash")
-- local logger = luastash.create_logger({name="mylogger", level="debug"})
-- @param options A table of options to configure the logger.
-- @return A new `Logger` instance.
return setmetatable({}, {
	__call = function(t, ...)
		return run_stash(...)
	end,
	__index = {
		create_logger = function(options)
			return Logger:new(options)
		end,
		INPUT_CONTINUE_SIGNAL = INPUT_CONTINUE_SIGNAL
	}
})
