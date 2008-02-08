#!/usr/bin/ruby

if File.symlink?(__FILE__)
  $:.insert($:.size-1,File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__)))
else
  $:.insert($:.size-1,File.dirname(__FILE__)) << File.dirname(__FILE__) unless $:.include?(File.dirname(__FILE__))
end

require 'jetusagereport.rb'
require "getoptlong"
require "date"

# Set the options for this script
opts= GetoptLong.new(
  ["-d", GetoptLong::REQUIRED_ARGUMENT],
  ["-a", GetoptLong::REQUIRED_ARGUMENT],
  ["-u", GetoptLong::REQUIRED_ARGUMENT],
  ["-s", GetoptLong::REQUIRED_ARGUMENT],
  ["-h", GetoptLong::NO_ARGUMENT]
)

# Initialize the list of users and projects 
users=""
projects=""

# Initialize the start and end times to today at 00:00:00 thru current time
now=Time.now
start_str=Time.gm(now.year,now.month,now.day).strftime("%y-%m-%d_%H:%M:%S")
end_str=now.strftime("%y-%m-%d_%H:%M:%S")

# Parse the options
opts.each { |option,arg|

  if option=="-d"
    start_str,end_str=arg.split(/,/)
  end

  if option=="-u"
    users=arg.split(/,/)
  end

  if option=="-a"
    projects=arg.split(/,/)
  end

  if option=="-s"
    ENV['SGE_ROOT']=arg
  end

}

if ENV['SGE_ROOT'].nil?
  raise "$SGE_ROOT is not set.  Use the -s option to specify a value for it."
end

report=JetUsageReport.new(start_str,end_str,users,projects)
report.print_full_report

