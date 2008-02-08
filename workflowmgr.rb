#!/usr/bin/ruby

if File.symlink?(__FILE__)
  $:.insert($:.size-1,File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__)))
else
  $:.insert($:.size-1,File.dirname(__FILE__)) << File.dirname(__FILE__) unless $:.include?(File.dirname(__FILE__))
end

require 'optparse'
require 'workflow.rb'

UPDATE_INTERVAL=60

xmlfile=nil
storefile=nil
haltfile=nil
shutdown=false
shutdowncycles=nil
doloop=false

ctrl_opts={ 
          :retries => 1000000,          
          :sleep_inc => 2,
          :min_sleep => 2, 
          :max_sleep => 10,
          :max_age => 900,
          :suspend => 30,
          :refresh => 5,
          :timeout => 45,
          :poll_retries => 16,
          :poll_max_sleep => 0.08,
          :debug => false
}

# Define the valid options
opts=OptionParser.new
opts.on("-x","--xml","=XML_FILE",
        "The full path of the XML workflow description file.\n",
        String) { |val| xmlfile=val }
opts.on("-s","--store","=STORE_FILE", 
        "The path of the file used to store the workflow's state.\n",
        String) { |val| storefile=val }
opts.on("--halt","=[YYYYMMDDHH,...]",
        /(\d){10}(,\d{10})*/,
        "Halt each cycle in the cycle list.  The cycle list is specified",
        "as a string of comma separated cycles in yyyymmddhh format.",
        "All running jobs for each cycle in the cycle list are killed.",
        "All workflow progress for each cycle in the cycle list is erased.",
        "The cycles can be resumed later from the beginning if desired.",
        "If a cycle list is not specified, all cycles are halted.\n",
        Array) { |cycles|
                 shutdown=true
                 unless cycles.nil? 
                   shutdowncycles=cycles.collect { |cycle| Time.gm(cycle[0..3],cycle[4..5],cycle[6..7],cycle[8..9]) }
                 end
               }
opts.on("--loop",
        "Run the workflowmgr in an infinite loop.") { |val| doloop=true }

# Parse the options and display usage if options are invalid
begin
  opts.parse(*ARGV)
  raise if xmlfile.nil?
  raise if storefile.nil?
rescue
  puts opts.to_s
  exit
end

# Process the workflow
if shutdown
  workflow=Workflow.new(xmlfile,storefile,ctrl_opts)
  workflow.halt(shutdowncycles,ctrl_opts)
#  shutdowncycles.each { |cycle|
#    File.new("#{File.dirname(xmlfile)}/HALTED_#{cycle.getgm.strftime('%Y%m%d%H')}","w")
#  }
else
  loop do
    workflow=Workflow.new(xmlfile,storefile)
    workflow.run
    break unless doloop
    break if workflow.done?
    sleep UPDATE_INTERVAL
  end
end
