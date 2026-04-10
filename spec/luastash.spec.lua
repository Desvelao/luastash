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

local function input_integers_continue(options)
	local index = 1
	local values = options.values
	local continue_generator = 0
	local function test()
		local value = values[index]
		index = index + 1

		if value == nil then
			continue_generator = continue_generator + 1
			if options.continues > continue_generator then
				return true, nil -- allow the input run, but the data process will be skipped
			end
		end
		return value ~= nil, value
	end

	return test
end

local function increase_integer(options, event)
	event.data = event.data + options.value
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
					assert.stub(t.print).was.called_with(event.data)
				end,
			},
		})
	end)

	it("Filters stash", function()
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
					assert.stub(t.print).was.called_with(event.data)
				end,
			},
		})
	end)

	it("Skip input stash", function()
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
						continues = 4,
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
				["input-test"] = input_integers_continue,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
					assert.stub(t.print).was.called_with(event.data)
				end,
			},
		})
	end)

	it("Run multiple inputs stash", function()
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
						continues = 4,
					},
				},
				{
					type = "input-test",
					options = {
						values = { 2, 3, 4, 5, 6 },
						continues = 3,
					},
				},
				{
					type = "input-test",
					options = {
						values = { 10, 20, 30, 40 },
						continues = 3,
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
				["input-test"] = input_integers_continue,
			},
			outputs = {
				["output-test"] = function(options, event)
					t.print(event.data)
					assert.stub(t.print).was.called_with(event.data)
				end,
			},
		})
	end)
end)
