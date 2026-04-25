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

# Create workflow engine and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV,
                                             'rocotorun', # command name (used for messages)
                                             'run', # what the command does (used for messages)
                                             default_all: true) # default task and cycle selection is everything
workflow_engine = WorkflowMgr::WorkflowEngine.new(opt)
workflow_engine.run
