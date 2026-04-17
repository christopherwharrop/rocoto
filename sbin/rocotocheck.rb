#!/apps/ruby/3.2.3/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Set up standalone bundle (no bundler gem required at runtime)
require_relative '../bundle/bundler/setup'

# Add include path for WFM libraries
$:.unshift("#{__WFMDIR__}/lib")

# Load workflow status library
require 'wfmstat/statusengine'
require 'workflowmgr/workflowsubsetoptions'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status engine and run it
opt=WorkflowMgr::WorkflowSubsetOptions.new(ARGV,
      name='rocotocheck', # command name (used for messages)
      action='check',     # what the command does (used for messages)
      default_all=false)  # task and cycle are required
statusEngine=WFMStat::StatusEngine.new(opt)
statusEngine.checkTasks

