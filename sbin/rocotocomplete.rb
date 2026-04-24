#!/usr/bin/env ruby

# Get the base directory of the WFM installation
rocoto_dir = File.expand_path('..', __dir__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$LOAD_PATH.unshift("#{rocoto_dir}/lib")

# Load workflow status library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'

# Create workflow status and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV,
                                             'rocotocomplete', # command name (used for messages)
                                             'completion', # what the command does (used for messages)
                                             default_all: true) # default task and cycle selection is everything
workflow_engine = WorkflowMgr::WorkflowEngine.new(opt)
workflow_engine.complete!
