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
  Rake::Task[:spec].reenable
  Rake::Task[:spec].invoke
end

# Task to run specs with coverage shown in terminal
desc 'Run specs with coverage report in terminal'
task :coverage_terminal do
  ENV['COVERAGE'] = 'true'
  ENV['COVERAGE_TERMINAL'] = 'true'
  Rake::Task[:spec].reenable
  Rake::Task[:spec].invoke
end

# RuboCop tasks
begin
  require 'rubocop/rake_task'

  RuboCop::RakeTask.new(:rubocop) do |task|
    task.options = ['--display-cop-names']
  end

  desc 'Run RuboCop with auto-correct'
  task :rubocop_fix do
    sh 'bundle exec rubocop -a'
  end
rescue LoadError
  # RuboCop not available
end

# Make spec the default task
task default: :spec

desc 'List all rake tasks'
task :tasks do
  puts 'Available tasks:'
  puts '  rake spec              - Run all specs'
  puts '  rake coverage          - Run specs with HTML coverage report'
  puts '  rake coverage_terminal - Run specs with coverage shown in terminal'
  puts '  rake -T                - List all tasks'
end
