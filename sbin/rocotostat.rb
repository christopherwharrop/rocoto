#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/SystemTimer")

# Load workflow status library
require 'wfmstat/statusengine'
require 'wfmstat/wfmstatoption'
require 'workflowmgr/workflowconfig'

# Set the Rocoto config and options
WorkflowMgr.options_set(WorkflowMgr::WorkflowOption.new(ARGV))
WorkflowMgr.config_set(WorkflowMgr::WorkflowYAMLConfig.new)

# Create workflow status and run it
if WorkflowMgr::OPTIONS.verbose > 999
  set_trace_func proc { |event,file,line,id,binding,classname| printf "%10s %s:%-2d %10s %8s\n",event,file,line,id,classname }
end
statusEngine=WFMStat::StatusEngine.new(WorkflowMgr::OPTIONS)
statusEngine.wfmstat

