#!/usr/bin/env ruby

# Get the base directory of the WFM installation
rocoto_dir = File.expand_path('..', __dir__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$LOAD_PATH.unshift("#{rocoto_dir}/lib")

# Load workflow engine library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'
require 'workflowmgr/utilities'

# Replace that ridiculous Libxml-ruby handler that automatically sends
# output to stderr We want to control what output goes where and when.
# LibXML::XML::Error.set_handler do |error|
#   WorkflowMgr.stderr(error.to_s)
#   WorkflowMgr.log(error.to_s)
# end

# Create workflow engine and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV, 'rocotorewind', 'rewind')
workflow_engine = WorkflowMgr::WorkflowEngine.new(opt)
workflow_engine.rewind!
