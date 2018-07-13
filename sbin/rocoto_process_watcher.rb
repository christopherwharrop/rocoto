#! /usr/bin/env ruby

require 'time'

cmd=[ '/bin/sleep', '60' ]
$childpid=nil
ROCOTO_JOBID=ENV['rocoto_jobid']
ROCOTO_JOBDIR=ENV['rocoto_jobdir']
ROCOTO_TICKTIME=ENV['rocoto_ticktime'].to_i
ROCOTO_JOBLOG="#{ROCOTO_JOBDIR}/#{ROCOTO_JOBID}.log"

if not File.directory? ROCOTO_JOBDIR
  Dir.mkdir ROCOTO_JOBDIR
end

def message(s)
  datestr=Time.now.to_i
  open(ROCOTO_JOBLOG,'a') do |f|
    f.puts("@ #{datestr} job #{ROCOTO_JOBID} : #{s}")
  end
end

def handle_signal(i)
  message "SIGNAL #{i}"
  begin
    kill $childpid
  ensure
    exit -i
  end
end

def make_handler(i)
  trap(i) { handle_signal i }
end


message("COMMAND #{cmd.join(' ').inspect}")
begin
  $childpid=fork {
    exec(*cmd)
  }
  [ 2, 3, 13, 15, 10, 12 ].each do |i|
    make_handler i
  end
  if $childpid.nil?
    message "FAIL CANNOT RUN COMMAND"
  end

  message("START #{$childpid}")
  mark=Time.now.to_i
  status=nil
  loop do
    status=Process.waitpid($childpid,Process::WNOHANG)
    break if not status.nil?
    if Time.now.to_f >= mark+ROCOTO_TICKTIME-0.01
      message("RUNNING #{$childpid}")
      mark=Time.now.to_i
    end
    sleep(10)
  end
  
  message "EXIT #{status.to_i}"
ensure
  message "HANDLER COMPLETE"
end
