unless defined? $__account__

##########################################
#
# Class Account
#
##########################################
class Account

  attr_reader :name
  attr_reader :maxwalltime
  attr_reader :defwalltime
  attr_reader :maxcputime
  attr_reader :maxcpus
  attr_reader :maxjobcpus
  attr_reader :maxjobs
  attr_reader :defaultpri
  attr_reader :maxpri
  attr_reader :maxnodepri
  attr_reader :shorttime
  attr_reader :nodeprop
  attr_reader :data
  attr_reader :dquota

  # Class variables
  @@resource_file="/usr/local/fsl/etc/resource_control"

  #####################################################
  #
  # new
  #
  #####################################################
  def Account.new(name)

    begin

      # Make sure the account is valid
#      if !(IO.readlines(@@resource_file,nil)[0]=~/^\s*#{name}\./)
#	raise "Sorry, the project '#{name}' does not exist!"
#      end

      valid=false
      IO.foreach(@@resource_file) do |line|
        if (line=~/^\s*#{name}\./)
          valid=true
          break
        end
      end      
      raise "Sorry, the project '#{name}' does not exist!" unless valid

      if `/usr/bin/id -Gn #{ENV['LOGNAME']} | grep -w jetmgmt`.empty? && Process.euid != 0
        if `/usr/bin/id -Gn #{ENV['LOGNAME']} | grep -w #{name}`.empty?
  	  raise "Sorry, either you are not a member of project '#{name}' " +
                "or project '#{name}' does not exist"
        end
      end

      super(name)

    rescue
      raise $!
  
    end
    
  end

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(name)

    begin

      @name=name
    
      @maxwalltime=0
      @defwalltime=0
      @maxcputime=0
      @maxcpus=0
      @maxjobcpus=0
      @maxjobs=0
      @defaultpri=0
      @maxpri=0
      @maxnodepri=0
      @shorttime=0
      @nodeprop=["None"]
      @data=["None"]
      @dquota=[0]

      # Get default account restrictions
      IO.foreach(@@resource_file) do |line|
        if line=~/^\s*default\.maxwalltime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @maxwalltime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*default\.defwalltime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @defwalltime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*default\.maxcputime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @maxcputime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*default\.maxcpus\s*=\s*(\d+)\s*$/
          @maxcpus=$1.to_i
        end
        if line=~/^\s*default\.maxjobcpus\s*=\s*(\d+)\s*$/
          @maxjobcpus=$1.to_i
        end
        if line=~/^\s*default\.maxjobs\s*=\s*(\d+)\s*$/
          @maxjobs=$1.to_i
        end
        if line=~/^\s*default\.defaultpri\s*=\s*(\d+)\s*$/
          @defaultpri=$1.to_i
        end
        if line=~/^\s*default\.maxpri\s*=\s*(\d+)\s*$/
          @maxpri=$1.to_i
        end
        if line=~/^\s*default\.maxnodepri\s*=\s*(\d+)\s*$/
          @maxnodepri=$1.to_i
        end
        if line=~/^\s*default\.shorttime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
  	@shorttime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*default\.nodeprop\s*=\s*(\S+)\s*$/
          @nodeprop=$1.split(':')
        end
        if line=~/^\s*default\.data\s*=\s*(\S+)\s*$/
          @data=$1.split(',')
        end
        if line=~/^\s*default\.dquota\s*=\s*(\S+)\s*$/
          @dquota=$1.split(',')
        end
      end

      # Get account restrictions
      IO.foreach(@@resource_file) do |line|
        if line=~/^\s*#{name}\.maxwalltime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @maxwalltime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*#{name}\.defwalltime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @defwalltime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*#{name}\.maxcputime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
          @maxcputime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*#{name}\.maxcpus\s*=\s*(\d+)\s*$/
          @maxcpus=$1.to_i
        end
        if line=~/^\s*#{name}\.maxjobcpus\s*=\s*(\d+)\s*$/
          @maxjobcpus=$1.to_i
        end
        if line=~/^\s*#{name}\.maxjobs\s*=\s*(\d+)\s*$/
          @maxjobs=$1.to_i
        end
        if line=~/^\s*#{name}\.defaultpri\s*=\s*(\d+)\s*$/
          @defaultpri=$1.to_i
        end
        if line=~/^\s*#{name}\.maxpri\s*=\s*(\d+)\s*$/
          @maxpri=$1.to_i
        end
        if line=~/^\s*#{name}\.maxnodepri\s*=\s*(\d+)\s*$/
          @maxnodepri=$1.to_i
        end
        if line=~/^\s*#{name}\.shorttime\s*=\s*(((\d+):)?(\d+):)?(\d+)\s*$/
  	  @shorttime=$3.to_i*3600 + $4.to_i*60 + $5.to_i
        end
        if line=~/^\s*#{name}\.nodeprop\s*=\s*([^+\s]+)\s*$/
          @nodeprop=$1.split(':')
        elsif line=~/^\s*#{name}\.nodeprop\s*=\s*\+\s*(\S+)\s*$/
	  @nodeprop=@nodeprop+$1.split(':')
        end
        if line=~/^\s*#{name}\.data\s*=\s*(\S+)\s*$/
          @data=$1.split(',')
        end
        if line=~/^\s*#{name}\.dquota\s*=\s*(\S+)\s*$/
          @dquota=$1.split(',')
        end
      end
 
    rescue
      raise $!

    end

  end

  #####################################################
  #
  # print_params
  #
  #####################################################
  def print_params

    
    puts "-------------------------------------------------------"
    puts "Account Name: #{@name}"
    puts "-------------------------------------------------------"

    hours=@maxwalltime/3600
    minutes=(@maxwalltime - hours*3600)/60
    seconds=@maxwalltime - hours*3600 - minutes*60
#    puts "Maximum Wall Clock Time: #{Time.at(@maxwalltime).strftime('%H:%M:%S')}"
    printf "Maximum Wall Clock Time: %02d:%02d:%02d\n",hours,minutes,seconds

    hours=@defwalltime/3600
    minutes=(@defwalltime - hours*3600)/60
    seconds=@defwalltime - hours*3600 - minutes*60
#    puts "Default Wall Clock Time: #{Time.at(@defwalltime).strftime('%H:%M:%S')}"
    printf "Default Wall Clock Time: %02d:%02d:%02d\n",hours,minutes,seconds

    hours=@maxcputime/3600
    minutes=(@maxcputime - hours*3600)/60
    seconds=@maxcputime - hours*3600 - minutes*60
#    puts "Maximum CPU Time:        #{Time.at(@maxcputime).strftime('%H:%M:%S')}"
    printf "Maximum CPU Time:        %02d:%02d:%02d\n",hours,minutes,seconds

    puts "-------------------------------------------------------"
    puts "Maximum # Of Running Jobs: #{@maxjobs}"
    puts "Maximum # Of CPUs In Use:  #{@maxcpus}"
    puts "Maximum # Of CPUs Per Job: #{@maxjobcpus}"
    puts "-------------------------------------------------------"
    puts "Maximum Job Priority: #{@maxpri}"
    puts "Default Job Priority: #{@defaultpri}"
    puts "-------------------------------------------------------"
    puts "Node properties allowed: #{@nodeprop.join(':')}"
    puts "-------------------------------------------------------"
    puts "File systems allocated:  #{@data.join(',')}"
    puts "File system quotas (GB): #{@dquota.join(',')}"
    puts "-------------------------------------------------------"

  end 


end

$__account__ == __FILE__
end
