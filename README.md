# luastash

## Description

This library provides a sequentional processor to transform data. This is inspired by the Logstash from Elasticsearch.

The data process has 3 phases:

1. Inputs
2. Filters
3. Outputs


## Features

- Retrieve the data from multiple inputs in a round-robin selection
- Transform the data optionally using filters
- Output the data in different ways such as console, files, send requests, etc...
- Run conditional processors defining a condition in Lua language using the `data` and context (`ctx`) variables
- Run on-failure pipeline in filters or outputs phases
- Skip running processors

## Installation

```console
luarocks install luastash
```

## Phases

### Input

Define the inputs to take the values to process.

It allows to define multiple input sources and this emits a value in a round-robin of the inputs.

If any input emits a nil value, then the processor will be considered as finished and this will be removed from the inputs list.
If you want to "pass" to the following input, emit the `"__INPUT_CONTINUE_SIGNAL__"` value.

### Filters

They are optionals and are used to transfom the data. It uses the under the hood the [pipeflow](https://github.com/Desvelao/pipeflow) library.

### Outputs

They define the output of the transformed data such as print the result, save to file, send request to external services, etc...

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
            ["if"] = "data > 4",
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
        ['generate-integers'] = function (options)
            local index = 1
            local values = options.values
            local function next()
                local value = values[index]
                index = index + 1
                return value
            end

            return next -- Return an iterator that is called and returns a value each time. When this returns nil, then the input will be removed of the input list.
        end,
        ['generate-integers-infinite-loop'] = function (options)
            local index = 1
            local values = options.values
            local function next()
                local value = values[index]
                index = index + 1
                if value == nil then
                    index = 1 -- Return to first item, in the following call this will return the first item
                    return luastash.INPUT_CONTINUE_SIGNAL -- No emit value, and the generator will not be removed. 
                    -- Both creates an infinite loop, so the generator never finished
                end
                return value
            end

            return next -- Return an iterator that is called and returns a value each time. When this returns nil, then the input will be removed of the input list.
        end
    },
    filters = {
        increase = function increase_integer(options, data)
            return data + options.value -- Ensure to return the data despite this is not modified
        end
    },
    outputs = {
        ['output-console'] = function(options, data)
            print(data)
            return data -- Ensure to return the data
        end
    }
}

luastash(
    pipeline,
    processors,
    -- optionally the logger can be passed or this will use the built-in logger instead
)

luastash(
    'pipeline.json', -- the pipeline can be defined in a JSON file
    processors,
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

## Run tests

```
/usr/local/bin/busted
```

## Format code

```
/usr/local/bin/stylua luastash.lua
```
