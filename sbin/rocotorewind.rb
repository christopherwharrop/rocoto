#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/rubysl-date/lib")
$:.unshift("#{__WFMDIR__}/lib/rubysl-parsedate/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/open4/lib")
$:.unshift("#{__WFMDIR__}/lib/thread/lib")

# Load workflow engine library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowsubsetoptions'
require 'workflowmgr/utilities'
require 'libxml'

# Replace that ridiculous Libxml-ruby handler that automatically sends
# output to stderr We want to control what output goes where and when.
LibXML::XML::Error.set_handler do |error|
  WorkflowMgr.stderr(error.to_s)
  WorkflowMgr.log(error.to_s)
end

# Create workflow engine and run it
opt=WorkflowMgr::WorkflowSubsetOptions.new(ARGV,'rocotorewind','rewind')
workflowengine=WorkflowMgr::WorkflowEngine.new(opt)
workflowengine.rewind!
