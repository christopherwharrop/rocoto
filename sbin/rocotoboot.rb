#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/rubysl-date/lib")
$:.unshift("#{__WFMDIR__}/lib/rubysl-parsedate/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/SystemTimer")
$:.unshift("#{__WFMDIR__}/lib/open4/lib")

# Load workflow status library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowbootoption.rb'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when                                     
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status and run it
workflowEngine=WorkflowMgr::WorkflowEngine.new(WorkflowMgr::WorkflowBootOption.new(ARGV))
workflowEngine.boot

