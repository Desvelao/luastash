local luastash = require("luastash")

local function input_integers(options)
	local index = 1
	local values = options.values
	local function test()
		local value = values[index]
		index = index + 1

		return value
	end

	return test
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
				return '__INPUT_CONTINUE_SIGNAL__'
			end
		end
		return value
	end

	return test
end

local function increase_integer(options, data)
	return data + options.value
end

local function _print(options)
	print(options)
end

describe("Run stash", function()
	it("Basic stash", function()
		local t = {
			print = _print
		}

		stub(t, 'print')
		luastash(
			{
				inputs = {
					{
						type='input-test',
						options={
							values={1,2,3,4,5}
						}
					}
				},
				-- filters = {

				-- },
				outputs = {
					{
						type='output-test',
						options={}
					}
				},
			},
			{
				inputs = {
					['input-test'] = input_integers
				},
				-- filters = {

				-- },
				outputs = {
					['output-test'] = function(options, data)
						t.print(data)
						assert.stub(t.print).was.called_with(data)
					end
				}
			}
		)
		
	end)

	it("Filters stash", function()
		local t = {
			print = _print
		}

		stub(t, 'print')
		luastash(
			{
				inputs = {
					{
						type='input-test',
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
					}
				},
				outputs = {
					{
						type='output-test',
						options={}
					}
				},
			},
			{
				inputs = {
					['input-test'] = input_integers
				},
				filters = {
					increase = increase_integer
				},
				outputs = {
					['output-test'] = function(options, data)
						t.print(data)
						assert.stub(t.print).was.called_with(data)
					end
				}
			}
		)
		
	end)

	it("Skip input stash", function()
		local t = {
			print = _print
		}

		stub(t, 'print')
		luastash(
			{
				inputs = {
					{
						type='input-test',
						options={
							values={1,2,3,4,5},
							continues=4
						}
					}
				},
				-- filters = {

				-- },
				outputs = {
					{
						type='output-test',
						options={}
					}
				},
			},
			{
				inputs = {
					['input-test'] = input_integers_continue
				},
				-- filters = {

				-- },
				outputs = {
					['output-test'] = function(options, data)
						t.print(data)
						assert.stub(t.print).was.called_with(data)
					end
				}
			}
		)
		
	end)
end)
