#!/usr/bin/env ruby

# Get the base directory of the WFM installation
rocoto_dir = File.expand_path('..', __dir__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$LOAD_PATH.unshift("#{rocoto_dir}/lib")

# Load workflow status library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowvacuumoption'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
# LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Get vacuum options
opt = WorkflowMgr::WorkflowVacuumOption.new(ARGV)

# Are you sure?
printf "About to delete all jobs for cycles that completed or expired more than #{opt.age / 3600 / 24} days ago.\n\n"
printf "This is irreversible.  Are you sure? (y/n) "
reply = $stdin.gets
unless reply =~ /^[Yy]/
  Process.exit(0)
end

# Create workflow engine and vacuum
workflow_engine = WorkflowMgr::WorkflowEngine.new(opt)
workflow_engine.vacuum!(opt.age)
