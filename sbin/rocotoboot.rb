#!/usr/bin/env ruby

# Get the base directory of the WFM installation
__WFMDIR__ = File.expand_path('..', __dir__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$LOAD_PATH.unshift("#{__WFMDIR__}/lib")

# Load workflow status library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status and run it
opt = WorkflowMgr::WorkflowSubsetOptions.new(ARGV, 'rocotoboot', 'boot')
workflowEngine = WorkflowMgr::WorkflowEngine.new(opt)
workflowEngine.boot
