#!/usr/bin/ruby

require 'time'

$childpid=nil
ROCOTO_JOBID=ENV['ROCOTO_JOBID']
ROCOTO_JOBDIR=ENV['ROCOTO_JOBDIR']
ROCOTO_TICKTIME=ENV['ROCOTO_TICKTIME'].to_i
ROCOTO_JOBLOG="#{ROCOTO_JOBDIR}/#{ROCOTO_JOBID}.job"
ROCOTO_KILLFILE="#{ROCOTO_JOBDIR}/#{ROCOTO_JOBID}.kill"
COMMAND=ARGV

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
    message("KILL #{$childpid}")
    Process.kill $childpid
    Process.wait
  ensure
    exit -i
  end
end

def make_handler(i)
  trap(i) { handle_signal i }
end

running=false

message("COMMAND #{COMMAND.inspect}")
begin
  $childpid=fork {
    message("COMMAND #{COMMAND.inspect}")
    exec(COMMAND[0],*COMMAND[1..-1])
  }
  running=true

  if $childpid.nil?
    message "FAIL CANNOT RUN COMMAND"
  end

  # Important implementation detail: do not add handlers until after
  # forking.  If you add the handlers first, then the child process
  # will ignore these signals.
  [ 2, 3, 13, 15, 10, 12 ].each do |i|
    make_handler i
  end

  message("START #{$childpid}")
  mark=Time.now.to_i
  status=nil
  killed=false
  loop do
    if not killed and File.exists? ROCOTO_KILLFILE
      message("KILL #{$childpid}")
      Process.kill(15,$childpid)
      Process.wait
      exit
    end
    pid2, status = Process.waitpid2($childpid,Process::WNOHANG)
    break if not status.nil?
    if Time.now.to_f >= mark+ROCOTO_TICKTIME-0.01
      message("RUNNING #{$childpid}")
      mark=Time.now.to_i
    end
    sleep(10)
  end
  
  message "EXIT #{status.exitstatus}"
ensure
  message "HANDLER COMPLETE"
end
