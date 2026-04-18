# frozen_string_literal: true

require 'bundler/setup'
require 'rspec/core/rake_task'

# Default task runs all specs
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = '--format documentation'
end

# Task to run specs with coverage
desc 'Run specs with coverage report'
task :coverage do
  ENV['COVERAGE'] = 'true'
  Rake::Task[:spec].invoke
end

# Make spec the default task
task default: :spec

desc 'List all rake tasks'
task :tasks do
  puts 'Available tasks:'
  puts '  rake spec     - Run all specs'
  puts '  rake coverage - Run specs with coverage report'
  puts '  rake -T       - List all tasks'
end
