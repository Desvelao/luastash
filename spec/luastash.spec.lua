local luastash = require("luastash")

local function input_integers_co(options)
	local index = 1
	local values = options.values
	return coroutine.wrap(function(t)
		while true do
			local value = values[index]
			coroutine.yield(value ~= nil, value)
			index = index + 1
		end
	end)
end

local function input_integers_no_emit_from_index(options)
	local index = 1
	local values = options.values
	local continue_generator = 0
	local function test()
		if index >= options.no_emit_from_index then
			return false, nil -- stop the input run, and the data process will be skipped
		end
		local value = values[index]
		index = index + 1
		return value ~= nil, value
	end

	return test
end

local function increase_integer(options, event)
	event.data = event.data + options.value
	return event
end

local function filter_manage_metadata(options, event)
	event:set_metadata(options.key, event.data)
	return event
end

local function filter_manage_data(options, event)
	event:set(options.key, event:get(options.concat_prop) .. options.value) -- this is just an example of how to set a value using the setter and get another value using the getter to create a new value
	return event
end

local function filter_accumulate_on_context(options, event, ctx)
	ctx.accumulator = ctx.accumulator + event.data
	return event
end

local function _print(options)
	print(options)
end

describe("Run stash", function()
	it("Basic stash", function()
		local t = {
			print = _print,
		}

		stub(t, "print")
		luastash({
			inputs = {
				{
					type = "input-test",
					options = {
						values = { 1, 2, 3, 4, 5 },
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, {
			inputs = {
				["input-test"] = input_integers_co,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
				end,
			},
		})

		assert.stub(t.print).was.called_with(1)
		assert.stub(t.print).was.called_with(2)
		assert.stub(t.print).was.called_with(3)
		assert.stub(t.print).was.called_with(4)
		assert.stub(t.print).was.called_with(5)
	end)

	it("Basic stash with file pipeline definition", function()
		local t = {
			print = _print,
		}

		stub(t, "print")
		luastash(
			"spec/config.sample1.json",
			{ -- this assumes that the busted command is executed from the root of the project
				inputs = {
					["input-test"] = input_integers_co,
				},
				outputs = {
					["output-test"] = function(options, event)
						t.print(event.data)
					end,
				},
			}
		)

		assert.stub(t.print).was.called_with(1)
		assert.stub(t.print).was.called_with(2)
		assert.stub(t.print).was.called_with(3)
		assert.stub(t.print).was.called_with(4)
		assert.stub(t.print).was.called_with(5)
	end)

	it("Filters stash", function()
		local events = {}
		local t = {
			print = _print,
		}

		stub(t, "print")
		luastash({
			inputs = {
				{
					type = "input-test",
					options = {
						values = { 1, 2, 3, 4, 5 },
					},
				},
			},
			filters = {
				{
					type = "increase",
					options = {
						value = 2,
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, {
			inputs = {
				["input-test"] = input_integers_co,
			},
			filters = {
				increase = increase_integer,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
					table.insert(events, event)
				end,
			},
		})
		for _, event in ipairs(events) do
			assert.stub(t.print).was.called_with(event.data)
		end
	end)

	it("Skip events", function()
		local t = {
			print = _print,
		}

		stub(t, "print")
		luastash({
			inputs = {
				{
					type = "input-test",
					options = {
						values = { 1, 2, 3, 4, 5 },
						no_emit_from_index = 4,
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, {
			inputs = {
				["input-test"] = input_integers_no_emit_from_index,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
				end,
			},
		})
		assert.stub(t.print).was.called_with(1)
		assert.stub(t.print).was.called_with(2)
		assert.stub(t.print).was.called_with(3)
	end)

	it("Accumulate on context", function()
		local t = {
			print = _print,
		}

		local context = {
			my_context_value = "test",
			accumulator = 0,
		}

		stub(t, "print")
		luastash(
			{
				inputs = {
					{
						type = "input-test",
						options = {
							values = { 1, 2, 3, 4, 5 },
						},
					},
				},
				filters = {
					{
						type = "filter-accumulate-on-context",
						options = {},
					},
				},
				outputs = {
					{
						type = "output-test",
						options = {},
					},
				},
			},
			{ -- this assumes that the busted command is executed from the root of the project
				inputs = {
					["input-test"] = input_integers_co,
				},
				filters = {
					["filter-accumulate-on-context"] = filter_accumulate_on_context,
				},
				outputs = {
					["output-test"] = function(options, event)
						t.print(event.data)
					end,
				},
			},
			context -- context can be passed as the third argument of the luastash function and this will be passed to the processors in the ctx parameter
		)
		assert.stub(t.print).was.called_with(1)
		assert.stub(t.print).was.called_with(2)
		assert.stub(t.print).was.called_with(3)
		assert.stub(t.print).was.called_with(4)
		assert.stub(t.print).was.called_with(5)
		assert.are.equals(context.accumulator, 15) -- the filter_accumulate_on_context filter should have accumulated the values in the context
	end)

	it("Event has metadata fields and source and source_tag", function()
		local events = {}
		luastash({
			inputs = {
				{
					type = "input-test",
					tag = "my_tag",
					options = {
						values = { 1, 2, 3, 4, 5 },
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, { -- this assumes that the busted command is executed from the root of the project
			inputs = {
				["input-test"] = input_integers_co,
			},
			outputs = {
				["output-test"] = function(options, event)
					table.insert(events, event)
				end,
			},
		})

		for _, event in ipairs(events) do
			assert.are.equals("input-test", event:get_metadata("source"))
			assert.are.equals("input-test", event.metadata.source)
			assert.are.equals("my_tag", event:get_metadata("source_tag"))
			assert.are.equals("my_tag", event.metadata.source_tag)
			assert.is_not_nil(event:get_metadata("@timestamp"))
			assert.is_not_nil(event.metadata["@timestamp"])
			local ts = event:get_metadata("@timestamp")
			assert.is_truthy(ts:match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$"))
			assert.are.equals("1", event:get_metadata("@version"))
			assert.are.equals("1", event.metadata["@version"])
		end
	end)

	it("Event has custom metadata field defined with key and event.data value", function()
		local events = {}
		luastash({
			inputs = {
				{
					type = "input-test",
					tag = "my_tag",
					options = {
						values = { 1, 2, 3, 4, 5 },
					},
				},
			},
			filters = {
				{
					type = "filter-manage-metadata",
					options = {
						key = "my_custom_key",
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, { -- this assumes that the busted command is executed from the root of the project
			inputs = {
				["input-test"] = input_integers_co,
			},
			filters = {
				["filter-manage-metadata"] = filter_manage_metadata,
			},
			outputs = {
				["output-test"] = function(options, event)
					table.insert(events, event)
				end,
			},
		})

		for _, event in ipairs(events) do
			assert.are.equals("input-test", event:get_metadata("source"))
			assert.are.equals("input-test", event.metadata.source)
			assert.are.equals("my_tag", event:get_metadata("source_tag"))
			assert.are.equals("my_tag", event.metadata.source_tag)
			assert.is_not_nil(event:get_metadata("@timestamp"))
			assert.is_not_nil(event.metadata["@timestamp"])
			local ts = event:get_metadata("@timestamp")
			assert.is_truthy(ts:match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$"))
			assert.are.equals("1", event:get_metadata("@version"))
			assert.are.equals("1", event.metadata["@version"])
			assert.are.equals(event.data, event:get_metadata("my_custom_key"))
			assert.are.equals(event.data, event.metadata.my_custom_key)
		end
	end)

	it("Filter define data using the setter and assert using the getter", function()
		local events = {}
		local key1 = "[my_custom_key][subkey]"
		local key2 = "[my_custom_key2][subkey][sub.key2]"
		luastash({
			inputs = {
				{
					type = "input-test",
					tag = "my_tag",
					options = {
						values = { { prop = "test" }, { prop = "test2" } },
					},
				},
			},
			filters = {
				{
					type = "filter-manage-data",
					options = {
						key = key1,
						value = "value",
						concat_prop = "[prop]",
						-- this is just an example of how to set a value using the setter and get
					},
				},
				{
					type = "filter-manage-data",
					options = {
						key = key2,
						value = "other_value",
						concat_prop = "[my_custom_key][subkey]",
						-- this is just an example of how to set a value using the setter and get
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, { -- this assumes that the busted command is executed from the root of the project
			inputs = {
				["input-test"] = input_integers_co,
			},
			filters = {
				["filter-manage-data"] = filter_manage_data,
			},
			outputs = {
				["output-test"] = function(options, event)
					table.insert(events, event)
				end,
			},
		})
		-- TODO: assert the event.data has tables in the defined keys
		assert.are.equals("testvalue", events[1]:get(key1))
		assert.are.equals("test", events[1]:get("[prop]"))
		assert.are.equals("testvalueother_value", events[1]:get(key2))
		assert.are.equals("test2value", events[2]:get(key1))
		assert.are.equals("test2", events[2]:get("[prop]"))
		assert.are.equals("test2valueother_value", events[2]:get(key2))
	end)

	it("Run multiple inputs stash with skip inputs and remove inputs", function()
		local t = {
			print = _print,
		}

		stub(t, "print")
		luastash({
			inputs = {
				{
					type = "input-test",
					options = {
						values = { 1, 2, 3, 4, 5 },
						no_emit_from_index = 4,
					},
				},
				{
					type = "input-test",
					options = {
						values = { 20, 30, 40, 50, 60 },
						no_emit_from_index = 3,
					},
				},
				{
					type = "input-test",
					options = {
						values = { 100, 200, 300, 400 },
						no_emit_from_index = 3,
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, {
			inputs = {
				["input-test"] = input_integers_no_emit_from_index,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
				end,
			},
		})
		assert.stub(t.print).was.called_with(1)
		assert.stub(t.print).was.called_with(2)
		assert.stub(t.print).was.called_with(3)
		assert.stub(t.print).was.called_with(20)
		assert.stub(t.print).was.called_with(30)
		assert.stub(t.print).was.called_with(100)
		assert.stub(t.print).was.called_with(200)
	end)

	it("Inputs sequence (last removed) of processed event with generators that are removed", function()
		local input_calls_gen = {}

		local function input_integers_no_emit_from_index(options)
			local index = 1
			local values = options.values
			local continue_generator = 0
			local function test()
				local value = values[index]
				table.insert(input_calls_gen, { tag = options.tag, index = index, value = value })
				if index >= options.no_emit_from_index then
					return false, nil -- stop the input run, and the data process will be skipped
				end
				index = index + 1
				return value ~= nil, value
			end

			return test
		end

		luastash({
			inputs = {
				{
					type = "input-test",
					options = {
						values = { 1, 2, 3, 4, 5 },
						no_emit_from_index = 4,
						tag = "input1",
					},
				},
				{
					type = "input-test",
					options = {
						values = { 10, 20, 30, 40, 50 },
						no_emit_from_index = 3,
						tag = "input2",
					},
				},
			},
			outputs = {
				{
					type = "output-test",
					options = {},
				},
			},
		}, {
			inputs = {
				["input-test"] = input_integers_no_emit_from_index,
			},
			outputs = {
				["output-test"] = function(options, event) end,
			},
		})

		assert.are.equals("input1", input_calls_gen[1].tag)
		assert.are.equals(1, input_calls_gen[1].index)
		assert.are.equals(1, input_calls_gen[1].value)
		assert.are.equals("input2", input_calls_gen[2].tag)
		assert.are.equals(1, input_calls_gen[2].index)
		assert.are.equals(10, input_calls_gen[2].value)
		assert.are.equals("input1", input_calls_gen[3].tag)
		assert.are.equals(2, input_calls_gen[3].index)
		assert.are.equals(2, input_calls_gen[3].value)
		assert.are.equals("input2", input_calls_gen[4].tag)
		assert.are.equals(2, input_calls_gen[4].index)
		assert.are.equals(20, input_calls_gen[4].value)
		assert.are.equals("input1", input_calls_gen[5].tag)
		assert.are.equals(3, input_calls_gen[5].index)
		assert.are.equals(3, input_calls_gen[5].value)
		assert.are.equals("input2", input_calls_gen[6].tag)
		assert.are.equals(3, input_calls_gen[6].index)
		assert.are.equals(30, input_calls_gen[6].value)
		assert.are.equals("input1", input_calls_gen[7].tag)
		assert.are.equals(4, input_calls_gen[7].index)
		assert.are.equals(4, input_calls_gen[7].value)
	end)

	it(
		"Inputs sequence (second-to-last removed before last) of processed event with generators that are removed",
		function()
			local input_calls_gen = {}

			local function input_integers_no_emit_from_index(options)
				local index = 1
				local values = options.values
				local continue_generator = 0
				local function test()
					if index >= options.no_emit_from_index then
						return false, nil -- stop the input run, and the data process will be skipped
					end
					local value = values[index]
					index = index + 1
					return value ~= nil, value
				end

				return test
			end

			luastash({
				inputs = {
					{
						type = "input-test",
						tag = "input1",
						options = {
							values = { 1, 2, 3, 4, 5 },
							no_emit_from_index = 5,
						},
					},
					{
						type = "input-test",
						tag = "input2",
						options = {
							values = { 10, 20, 30, 40, 50 },
							no_emit_from_index = 3,
						},
					},
					{
						type = "input-test",
						tag = "input3",
						options = {
							values = { 100, 200, 300, 400, 500, 600, 700 },
							no_emit_from_index = 7,
						},
					},
				},
				outputs = {
					{
						type = "output-test",
						options = {},
					},
				},
			}, {
				inputs = {
					["input-test"] = input_integers_no_emit_from_index,
				},
				outputs = {
					["output-test"] = function(options, event)
						table.insert(input_calls_gen, { tag = event.metadata.source_tag, value = event.data })
					end,
				},
			})

			assert.are.equals("input1", input_calls_gen[1].tag)
			assert.are.equals(1, input_calls_gen[1].value)
			assert.are.equals("input2", input_calls_gen[2].tag)
			assert.are.equals(10, input_calls_gen[2].value)
			assert.are.equals("input3", input_calls_gen[3].tag)
			assert.are.equals(100, input_calls_gen[3].value)
			assert.are.equals("input1", input_calls_gen[4].tag)
			assert.are.equals(2, input_calls_gen[4].value)
			assert.are.equals("input2", input_calls_gen[5].tag)
			assert.are.equals(20, input_calls_gen[5].value)
			assert.are.equals("input3", input_calls_gen[6].tag)
			assert.are.equals(200, input_calls_gen[6].value)
			assert.are.equals("input1", input_calls_gen[7].tag)
			assert.are.equals(3, input_calls_gen[7].value)
			assert.are.equals("input3", input_calls_gen[8].tag)
			assert.are.equals(300, input_calls_gen[8].value)
			assert.are.equals("input1", input_calls_gen[9].tag)
			assert.are.equals(4, input_calls_gen[9].value)
			assert.are.equals("input3", input_calls_gen[10].tag)
			assert.are.equals(400, input_calls_gen[10].value)
			assert.are.equals("input3", input_calls_gen[11].tag)
			assert.are.equals(500, input_calls_gen[11].value)
			assert.are.equals("input3", input_calls_gen[12].tag)
			assert.are.equals(600, input_calls_gen[12].value)
		end
	)
end)
