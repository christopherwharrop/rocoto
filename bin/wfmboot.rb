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

# Load workflow engine library
require 'workflowmgr/workflowengine'

WorkflowMgr::VERSION="1.0.b0"

# Create workflow engine and run it
workflowengine=WorkflowMgr::WorkflowEngine.new(ARGV)
workflowengine.run
