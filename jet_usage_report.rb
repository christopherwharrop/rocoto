#!/usr/bin/ruby

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/libxml-ruby-0.8.3/ext/libxml")

require 'jetusagereport.rb'
require "getoptlong"
require "date"

# Set the options for this script
opts= GetoptLong.new(
#  ["--users",      GetoptLong::REQUIRED_ARGUMENT],
#  ["--projects",   GetoptLong::REQUIRED_ARGUMENT],
  ["--sge-root",   GetoptLong::REQUIRED_ARGUMENT],
  ["--help",       GetoptLong::NO_ARGUMENT],
  ["--mail",       GetoptLong::REQUIRED_ARGUMENT],
  ["--start-time", GetoptLong::REQUIRED_ARGUMENT],
  ["--end-time",   GetoptLong::REQUIRED_ARGUMENT],
  ["--years",      GetoptLong::OPTIONAL_ARGUMENT],
  ["--months",     GetoptLong::OPTIONAL_ARGUMENT],
  ["--weeks",      GetoptLong::OPTIONAL_ARGUMENT],
  ["--days",       GetoptLong::OPTIONAL_ARGUMENT],
  ["--hours",      GetoptLong::OPTIONAL_ARGUMENT]
)

# Initialize the values of various options to nil
users=nil
projects=nil
email=nil
start_str=nil
end_str=nil
years=0
months=0
weeks=0
days=0
hours=0

# Parse the options
opts.each { |option,arg|

  if option=="--help"
    puts "jet_usage_report.rb [options]\n"
    puts
    puts "   --help                Print this message\n"
    puts "   --sge-root            Set the value of SGE_ROOT when running the report, used mostly when running from cron\n"
    puts "                         The default is the contents of $SGE_ROOT, which is always set except when run from cron\n"
    puts "   --mail [emp,project]  Email EMP, Project, or both EMP and Project, reports to PIs of those groups\n"
    puts "   --start-time          The beginning of the time interval for the report in yyyy-mm-dd_hh:mm:ss format\n"    
    puts "   --end-time            The end of the time interval for the report in yyyy-mm-dd_hh:mm:ss format\n"
    puts "   --years               The length in years of the report if either --start-time or --end-time is missing\n"
    puts "   --months              The length in months of the report if either --start-time or --end-time is missing\n"
    puts "   --weeks               The length in weeks of the report if either --start-time or --end-time is missing\n"
    puts "   --days                The length in days of the report if either --start-time or --end-time is missing\n"
    puts "   --hours               The length in hours of the report if either --start-time or --end-time is missing\n"
    puts
    exit
  end

#  if option=="--users"
#    users=arg.split(/,/)
#  end

#  if option=="--projects"
#    projects=arg.split(/,/)
#  end

  if option=="--sge-root"
    ENV['SGE_ROOT']=arg
  end

  if option=="--mail"
    email=arg.split(/,/)
  end

  if option=="--start-time"
    start_str=arg
  end

  if option=="--end-time"
    end_str=arg
  end

  if option=="--years"
    years=arg.empty? ? 1 : arg.to_i
  end

  if option=="--months"
    months=arg.empty? ? 1 : arg.to_i
  end

  if option=="--weeks"
    weeks=arg.empty? ? 1 : arg.to_i
  end

  if option=="--days"
    days=arg.empty? ? 1 : arg.to_i
  end

  if option=="--hours"
    hours=arg.empty? ? 1 : arg.to_i
  end

}

# Make sure we have SGE_ROOT in the environment
if ENV['SGE_ROOT'].nil?
  raise "$SGE_ROOT is not set.  Use the -s option to specify a value for it."
end

if start_str.nil? && end_str.nil?
  end_str=Time.now.strftime("%Y-%m-%d_%H:%M:%S")
end

# Set the start and end times
if !start_str.nil? && !end_str.nil?
  # If both start_str and end_str are already set, we already have a time interval
elsif !start_str.nil?
  # If only start_str is set, calculate end_str
  start_time=Time.gm(*(start_str.gsub(/[-_:]/,":").split(":")))
  end_time=DateTime.civil(start_time.year,start_time.month,start_time.day,start_time.hour,start_time.min,start_time.sec)
  end_time=end_time >> years*12
  end_time=end_time >> months
  end_time=end_time + (weeks * 7)
  end_time=end_time + days
  end_time=Time.gm(end_time.year,end_time.month,end_time.day,*(Date.day_fraction_to_time(end_time.day_fraction)))
  end_time+=hours*3600
  if start_time.to_i==end_time.to_i
    end_time+=(24*3600)
  end
  end_str=end_time.strftime("%Y-%m-%d_%H:%M:%S")
elsif !end_str.nil?
  # If only end_str is set, calculate start_str
  end_time=Time.gm(*(end_str.gsub(/[-_:]/,":").split(":")))
  start_time=DateTime.civil(end_time.year,end_time.month,end_time.day,end_time.hour,end_time.min,end_time.sec)
  start_time=start_time << years*12
  start_time=start_time << months
  start_time=start_time - (weeks * 7)
  start_time=start_time - days
  start_time=Time.gm(start_time.year,start_time.month,start_time.day,*(Date.day_fraction_to_time(start_time.day_fraction)))
  start_time-=hours*3600
  if start_time.to_i==end_time.to_i
    start_time-=(24*3600)
  end
  start_str=start_time.strftime("%Y-%m-%d_%H:%M:%S")
end

report=JetUsageReport.new(start_str,end_str,nil,nil)
if email.nil?
  report.print_full_report
else
  unless email.index("emp").nil?
    report.email_emp_reports
  end
  unless email.index("project").nil?
    report.email_project_reports
  end
end

