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
require 'workflowstatus/workflowdbstatus'

WorkflowMgr::VERSION="0.0.1"

# Create workflow status and run it
workflowdbstatus=WorkflowMgr::WorkflowDbStatus.new(ARGV)
workflowdbstatus.run

