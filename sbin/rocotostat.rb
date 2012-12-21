#!/usr/bin/ruby

# Get the base directory of the WFM installation
if File.symlink?(__FILE__)
  __WFMDIR__=File.dirname(File.dirname(File.expand_path(File.readlink(__FILE__),File.dirname(__FILE__))))
else
  __WFMDIR__=File.dirname(File.expand_path(File.dirname(__FILE__)))
end

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/SystemTimer")

# Load workflow status library
require 'wfmstat/statusengine'
require 'wfmstat/wfmstatoption'
require 'workflowmgr/workflowconfig'

# Set the Rocoto version
WorkflowMgr::VERSION=IO.readlines("#{__WFMDIR__}/VERSION",nil)[0]

# Set the Rocoto config and options
WorkflowMgr::OPTIONS=WFMStat::WFMStatOption.new(ARGV)
WorkflowMgr::CONFIG=WorkflowMgr::WorkflowYAMLConfig.new

# Create workflow status and run it
if WorkflowMgr::OPTIONS.verbose > 999
  set_trace_func proc { |event,file,line,id,binding,classname| printf "%10s %s:%-2d %10s %8s\n",event,file,line,id,classname }
end
statusEngine=WFMStat::StatusEngine.new(WorkflowMgr::OPTIONS)
statusEngine.wfmstat

