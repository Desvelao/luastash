# luastash

## Description

This library provides a sequential processor to transform data. It is inspired by Logstash, a data processing pipeline tool from Elasticsearch.

The data processing pipeline consists of three phases:

1. **Inputs**: Retrieve data from various sources.
2. **Filters**: Transform or filter the data.
3. **Outputs**: Send the processed data to different destinations.


## Features

- Retrieve data from multiple inputs using a round-robin selection.
- Optionally transform the data using filters.
- Output data in various ways, such as to the console, files, or by sending requests.
- Run conditional processors by defining conditions in Lua using the `data` and context (`ctx`) variables.
- Execute on-failure pipelines during the filters or outputs phases.
- Skip running specific processors when needed.

## Installation

```bash
luarocks install luastash
```

## Phases

### Inputs

Define the inputs to retrieve the values to process.

You can define multiple input sources, and the processor will emit values in a round-robin fashion from the inputs.

If any input emits a nil value, the processor will consider it finished and remove it from the inputs list.
To skip to the next input, emit the "__INPUT_CONTINUE_SIGNAL__" value.

### Filters

They are optionals and are used to transfom the data before sending it to the outputs.

It uses the under the hood the [pipeflow](https://github.com/Desvelao/pipeflow) library.

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
