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

# Load workflow status library
require 'workflowmgr/workflowengine'
require 'workflowmgr/workflowvacuumoption.rb'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when                                     
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Get vacuum options
opt=WorkflowMgr::WorkflowVacuumOption.new(ARGV)

# Are you sure?
printf "About to delete all jobs for cycles that completed or expired more than #{opt.age / 3600 / 24} days ago.\n\n"
printf "This is irreversible.  Are you sure? (y/n) "
reply=STDIN.gets
unless reply=~/^[Yy]/
  Process.exit(0)
end

# Create workflow engine and vacuum
workflowEngine=WorkflowMgr::WorkflowEngine.new(opt)
workflowEngine.vacuum!(opt.age)
