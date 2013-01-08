#!/usr/bin/ruby

# Get the base directory of the WFM installation
__WFMDIR__=File.expand_path("../../",__FILE__)

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/lib/libxml-ruby")
$:.unshift("#{__WFMDIR__}/lib/sqlite3-ruby")
$:.unshift("#{__WFMDIR__}/lib/SystemTimer")

# Load workflow status library
require 'wfmstat/statusengine'
require 'wfmstat/checktaskoption'

# Create workflow status engine and run it
statusEngine=WFMStat::StatusEngine.new(WFMStat::CheckTaskOption.new(ARGV))
statusEngine.checkTask

