#!/apps/ruby/3.2.3/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$:.unshift("#{__WFMDIR__}/lib")

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
