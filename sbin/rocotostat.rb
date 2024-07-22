#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and gem dependencies
$:.unshift("#{__WFMDIR__}/lib")
Dir["#{__WFMDIR__}/gems/gems/*"].each do |gemdir|
  next if gemdir == "." or gemdir == ".."
  $:.unshift("#{gemdir}/lib")
end

# Load workflow status library
require 'wfmstat/statusengine'
require 'wfmstat/wfmstatoption'
require 'libxml'

# Turn off that ridiculous Libxml-ruby handler that automatically sends output to stderr
# We want to control what output goes where and when
LibXML::XML::Error.set_handler(&LibXML::XML::Error::QUIET_HANDLER)

# Create workflow status engine and run it
opt=WFMStat::WFMStatOption.new(ARGV,'rocotostat','statting')
statusEngine=WFMStat::StatusEngine.new(opt)
statusEngine.wfmstat

