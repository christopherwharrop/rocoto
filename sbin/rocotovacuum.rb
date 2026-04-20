#!/usr/bin/env ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$:.unshift("#{__WFMDIR__}/lib")

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
