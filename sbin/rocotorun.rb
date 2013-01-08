#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/SystemTimer")

# Load workflow engine library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowoption'

# Create workflow engine and run it
workflowengine=WorkflowMgr::WorkflowEngine.new(WorkflowMgr::WorkflowOption.new(ARGV))
workflowengine.run

