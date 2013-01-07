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
require 'workflowmgr/workflowconfig'

# Set the Rocoto config and options
WorkflowMgr.options_set(WorkflowMgr::WorkflowOption.new(ARGV))
WorkflowMgr.config_set(WorkflowMgr::WorkflowYAMLConfig.new)

# Create workflow engine and run it
if WorkflowMgr::OPTIONS.verbose > 999
  set_trace_func proc { |event,file,line,id,binding,classname| printf "%10s %s:%-2d %10s %8s\n",event,file,line,id,classname }
end
workflowengine=WorkflowMgr::WorkflowEngine.new(WorkflowMgr::OPTIONS)
workflowengine.run

