# frozen_string_literal: true

# Configure SimpleCov for code coverage
if ENV['COVERAGE']
  require 'simplecov'
  SimpleCov.start do
    add_filter '/spec/'
    add_filter '/test/'
    add_filter '/vendor/'
    add_group 'Libraries', 'lib'
    minimum_coverage 45  # Adjusted for legacy codebase - will increase as we refactor
  end
end

# Add lib directory to load path
$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  # Use expect syntax (not should)
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # Configure output format
  config.default_formatter = 'doc' if config.files_to_run.one?

  # Run specs in random order to surface order dependencies
  config.order = :random
  Kernel.srand config.seed

  # Allow more verbose output when running a single spec file
  if config.files_to_run.one?
    config.default_formatter = 'doc'
  end
end
