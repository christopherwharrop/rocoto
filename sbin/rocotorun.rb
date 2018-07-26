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
$:.unshift("#{__WFMDIR__}/lib/thread/lib")

# Load workflow engine library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowbootoption'
require 'workflowmgr/utilities'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
#LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)
LibXML::XML::Error.set_handler do |error|
#  raise error
  WorkflowMgr.stderr(error.to_s)
  WorkflowMgr.log(error.to_s)
end

# Create workflow engine and run it
opt=WorkflowMgr::WorkflowBootOption.new(ARGV,
      name='rocotorun', # command name (used for messages)
      action='run',     # what the command does (used for messages)
      default_all=true) # default task and cycle selection is everything
workflowengine=WorkflowMgr::WorkflowEngine.new(opt)
workflowengine.run
