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
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV,
                                             'rocotocomplete', # command name (used for messages)
                                             'completion', # what the command does (used for messages)
                                             default_all: true) # default task and cycle selection is everything
workflow_engine = WorkflowMgr::WorkflowEngine.new(opt)
workflow_engine.complete!
