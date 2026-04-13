# luastash

## Description

This library provides a sequential processor to transform data. It is inspired by Logstash, a data processing pipeline tool from Elasticsearch, and Elasticsearch ingest pipelie.

The data processing pipeline consists of three phases:

1. **Inputs**: Retrieve data from various sources.
2. **Filters**: Transform or filter the data.
3. **Outputs**: Send the processed data to different destinations.

## Features

- Retrieve data from multiple inputs using a round-robin selection.
- Optionally transform the data using filters.
- Output data in various destinations.
- Run conditional processors by defining conditions in Lua using the `event` and context (`ctx`) variables.
- Execute on-failure pipelines during the filter or output phases.
- Skip running specific processors when needed.

## Installation

```bash
luarocks install luastash
```

## Phases

### Inputs

Define the inputs to retrieve the values to process.

You can define multiple input sources, and the processor will emit values in a round-robin fashion from the inputs.

The input emit 2 values:
- should continue: emit `true` to continue or `false` if the input should be removed from the input list
- value: the raw value, if `nil` is emitted then the processor will be skipped

The emitted value by the input will be used to build an event where:
- `event.data`: the value emitted by the input
- `event.metadata['@timestamp']`: define the date when the value was emitted
- `event.metadata['@version']`: define the version of the event
- `event.metadata['source']`: define the proccesor name
- `event.metadata['source_tag']`: define an optional tag

### Filters

They are optionals and are used to transfom the data before sending it to the outputs.

It uses the under the hood the [pipeflow](https://github.com/Desvelao/pipeflow) library.

### Outputs

They define the destination of the processed event, for example, print to console, save to file, send request to external services, etc...

It uses the under the hood the [pipeflow](https://github.com/Desvelao/pipeflow) library.

# Usage

```lua
local luastash = require("luastash")

-- Define the pipeline
local pipeline = {
    inputs = {
        {
            type='generate-integers',
            options={
                values={1,2,3,4,5}
            }
        }
    },
    filters = {
        {
            type='increase',
            options={
                value=2
            }
        },
        {
            -- Run conditionally the processor
            ["if"] = "event.data > 4",
            type='increase',
            options={
                value=4
            }
        }
        {
            -- The filters can appear multiple times in the pipeline, this allows with a definition in the fitler processor, this can be applied multiple times.
            type='increase',
            options={
                value=3
            }
        }
    },
    outputs = {
        {
            type='output-console',
            options={}
        }
    },
}

-- Define processors
local proccesors = {
    inputs = {
        ['generate-integers-coroutine-gen'] = function (options)
            local index = 1
            local values = options.values
            return coroutine.wrap(function() -- Return an iterator as coroutine generator. This is the recommended way.
                while true do
                    local value = values[index]
                    coroutine.yield(value ~= nil, value) -- if the first value emitted is false, the input will be removed and the data will not processed. If the second value is nil, the data will not processed
                    index = index + 1
                end
            end)
        end,
        ['generate-integers'] = function (options)
            local index = 1
            local values = options.values
            local function next()
                local value = values[index]
                index = index + 1
                return value ~= nil, value
            end

            return next -- Return an iterator that is called and returns a value each time. When this returns nil, then the input will be removed of the input list.
        end,
        ['generate-integers-infinite-loop-coroutine-gen'] = function (options)

            local index = 1
            local values = options.values
            return coroutine.wrap(function() -- Return an iterator as coroutine generator. This is the recommended way.
                while true do
                    local value = values[index]
                    if value == nil then
                        index = 1 -- Return to first item, in the following call this will return the first value of values
                        coroutine.yield(true, nil)
                    else
                        coroutine.yield(value ~= nil, value)
                        index = index + 1
                    end
                end
            end)
        end,
        ['generate-integers-infinite-loop'] = function (options)
            local index = 1
            local values = options.values
            local function next()
                local value = values[index]
                index = index + 1
                if value == nil then
                    index = 1 -- Return to first item, in the following call this will return the first value of values
                    return true, nil -- No emit value, and the generator will not be removed. Skip the process of the data
                end
                return value ~= nil, value
            end

            return next -- Return an iterator that is called and returns a value each time. When this returns nil, then the input will be removed of the input list.
        end
    },
    filters = {
        increase = function increase_integer(options, event)
            event.data = event.data + option.value
            return event
        end
    },
    outputs = {
        ['output-console'] = function(options, event)
            print(data)
            return event -- Ensure to return the event
        end
    }
}

-- Basic usage
-- Pipeline and processors can be defined in Lua tables
luastash(
    pipeline,
    processors,
)

-- Basic usage: file pipeline definition
luastash(
    'pipeline.json', -- the pipeline can be defined in a JSON file
    processors,
)

-- Advanced usage: file pipeline definition with context and custom logger
luastash(
    'pipeline.json',
    processors,
    {str='context string', number=42}, -- context is an optional table that is passed to the processors and can be used to share data between them or evaluate conditions in the processors (using the `ctx` variable in the condition)
    {logger = luastash.Logger:new({ name = "my-pipeline", level = "info" })} -- provide a logger to the processors (optional, if not provided a default logger with debug will be used)
)

```

## Logger

The run pipeline and pipeflow manager can use a logger that has the following methods:

- debug, info, warn, error: define the level log methods
- get_logger: create a child logger used in the pipeline run

# Development

## Run environment

```
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml exec dev sh
```

## Install depedencies

```
luarocks install --only-deps luastash-*.rockspec
```

## Run tests

```
/usr/local/bin/busted
```

## Format code

```
/usr/local/bin/stylua luastash.lua spec/luastash.spec.lua
```

# Publish package

```
luarocks update luastash-*.rockspec --api-key=<API_KEY>
```
