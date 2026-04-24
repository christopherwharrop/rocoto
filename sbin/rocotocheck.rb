#!/usr/bin/env ruby

# Get the base directory of the WFM installation
rocoto_dir = File.expand_path('..', __dir__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$LOAD_PATH.unshift("#{rocoto_dir}/lib")

# Load workflow status library
require 'wfmstat/statusengine'
require 'workflowmgr/workflowsubsetoptions'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
# LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status engine and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV,
                                             'rocotocheck', # command name (used for messages)
                                             'check', # what the command does (used for messages)
                                             default_all: false) # task and cycle are required
status_engine = WFMStat::StatusEngine.new(opt)
status_engine.check_tasks
