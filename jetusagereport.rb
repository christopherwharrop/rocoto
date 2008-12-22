unless defined? $__jetusagereport__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/usr/lib64/ruby/site_ruby/1.8/x86_64-linux") 

##########################################
#
# Class JetUsageReport
#
##########################################
class JetUsageReport

  @@wjet_cores=1392+1952  # wjet + hjet
  @@ejet_cores=616
  @@ijet_cores=1154
  @@smtpserver="10.1.99.99"

  require 'sgebatchsystem.rb'
  require 'net/smtp'

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(stime,etime,users=nil,projects=nil)

    # Make sure $SGE_ROOT is defined
    sge_root=ENV['SGE_ROOT']
    if sge_root.nil?
      raise "$SGE_ROOT is not defined"
    else
      # Set the path to the SGE commands
      output=Command.run("#{sge_root}/util/arch")
      if output[1] != 0
        raise output[0]
      else
        bin=output[0].chomp
      end
      sge_path="#{sge_root}/bin/#{bin}"
    end

    # Figure out what machine this report is for
    output=Command.run("#{sge_path}/qconf -spl | grep comp")
    if output[1] != 0
      raise output[0]
    else
      pes=output[0].chomp
    end
    case pes
      when /ncomp/
        @machine="iJET"
        @ncores=@@ijet_cores
      when /ecomp/
        @machine="eJET"
        @ncores=@@ejet_cores
      when /wcomp/
        @machine="wJET"
        @ncores=@@wjet_cores
    end

    # Convert start and end time strings into Time objects
    @start_time=Time.gm(*(stime.gsub(/[-_:]/,":").split(":")))
    @end_time=Time.gm(*(etime.gsub(/[-_:]/,":").split(":")))
 
    # Compute number of wall hours for this time period    
    @nwallhours=(@end_time-@start_time)/3600.0

    # Compute cpuhour capacity for this time period
    @ncpuhours=@nwallhours*@ncores

    # Get a batch system object
    sge=SGEBatchSystem.new()

    # Get the job statistics from the accounting logs
    @stats=sge.collect_job_stats(stime,etime,projects,users).split(/%/)

    # Reconstitute overall_stats
    @overall_stats=Hash.new
    @total=Hash.new
    @total["njobs"]=0
    @total["ncpus"]=0
    @total["walltime"]=0
    @total["cputime"]=0
    @user=Hash.new
    @project=Hash.new
    @emp=Hash.new
    @stats.each { |stat|
      emp,project,user=stat.split(":")[0..2]
      njobs,ncpus,walltime,cputime=stat.split(":")[3..6].collect {|i| i.to_i}
      if @overall_stats[emp].nil?
        @overall_stats[emp]=Hash.new
      end
      if @overall_stats[emp][project].nil?
        @overall_stats[emp][project]=Hash.new
      end
      if @overall_stats[emp][project][user].nil?
        @overall_stats[emp][project][user]=Hash.new
      end

      # Accumulate stats per user per project per emp
      @overall_stats[emp][project][user]["njobs"]=njobs
      @overall_stats[emp][project][user]["ncpus"]=ncpus
      @overall_stats[emp][project][user]["walltime"]=walltime
      @overall_stats[emp][project][user]["cputime"]=cputime

      # Accumulate grand total stats
      @total["njobs"]+=njobs
      @total["ncpus"]+=ncpus
      @total["walltime"]+=walltime
      @total["cputime"]+=cputime
      
      # Accumulate total stats per user 
      if @user[user].nil? 
        @user[user]=Hash.new
        @user[user]["njobs"]=0
        @user[user]["ncpus"]=0
        @user[user]["walltime"]=0
        @user[user]["cputime"]=0
      end
      @user[user]["njobs"]+=njobs
      @user[user]["ncpus"]+=ncpus
      @user[user]["walltime"]+=walltime
      @user[user]["cputime"]+=cputime

      # Accumulate total stats per project 
      if @project[project].nil? 
        @project[project]=Hash.new
        @project[project]["njobs"]=0
        @project[project]["ncpus"]=0
        @project[project]["walltime"]=0
        @project[project]["cputime"]=0
      end
      @project[project]["njobs"]+=njobs
      @project[project]["ncpus"]+=ncpus
      @project[project]["walltime"]+=walltime
      @project[project]["cputime"]+=cputime

      # Accumulate total stats per emp
      if @emp[emp].nil? 
        @emp[emp]=Hash.new
        @emp[emp]["njobs"]=0
        @emp[emp]["ncpus"]=0
        @emp[emp]["walltime"]=0
        @emp[emp]["cputime"]=0
      end
      @emp[emp]["njobs"]+=njobs
      @emp[emp]["ncpus"]+=ncpus
      @emp[emp]["walltime"]+=walltime
      @emp[emp]["cputime"]+=cputime

    }

    # Get EMP allocations
    @emp.keys.each {|emp|
      @emp[emp]["allocation"]=0.0
    }
 
  end


  #####################################################
  #
  # print_header
  #
  #####################################################
  def print_header

    start_str=@start_time.strftime("%m/%d/%Y %H:%M:%S %Z")
    end_str=@end_time.strftime("%m/%d/%Y %H:%M:%S %Z")

    msg=sprintf  "\n"
    msg+=sprintf "#{@machine} Usage Report For: #{start_str} thru #{end_str}\n"
    msg+=sprintf  "\n"

    return msg

  end


  #####################################################
  #
  # print_summary
  #
  #####################################################
  def print_summary

    msg=sprintf  "\n"
    msg+=sprintf "Machine Capacity Summary\n"
    msg+=sprintf "----------------------------------------\n"
    msg+=sprintf " Number of CPUs in Service: %12d\n",@ncores
    msg+=sprintf "Wall Clock Time Span (hrs): %12.2f\n",@nwallhours
    msg+=sprintf "CPU Time Capacity (cpuhrs): %12.2f\n",@ncpuhours
    msg+=sprintf "\n"
    msg+=sprintf "\n"
    msg+=sprintf "Machine Usage Summary\n"
    msg+=sprintf "----------------------------------------------\n"
    msg+=sprintf "  Total Number of Jobs Completed: %12d\n",@total["njobs"]
    msg+=sprintf "       Total Number of CPUs Used: %12d\n",@total["ncpus"]
    msg+=sprintf "Total Wall Clock Time Used (hrs): %12.2f\n",@total["walltime"]/3600.0
    msg+=sprintf "    Total CPU Time Used (cpuhrs): %12.2f\n",@total["cputime"]/3600.0
    msg+=sprintf "             Total %% Utilization: %12.2f\n",(@total["cputime"]/3600.0)/@ncpuhours*100
    msg+=sprintf "\n"

    return msg

  end


  #####################################################
  #
  # print_legend
  #
  #####################################################
  def print_legend

    msg=sprintf  "\n"
    msg+=sprintf "Utilization Definitions\n"
    msg+=sprintf "-----------------------------------------------------------------------------------------------\n"
    msg+=sprintf " Relative %% Utilization: The percent of the total *usage* of the machine that was used by\n"
    msg+=sprintf "                         the given EMP, project, or user, during the given time interval\n"
    msg+=sprintf " Absolute %% Utilization: The percent of the total *capacity* of the machine that was used\n"
    msg+=sprintf "                         during the given time interval\n"
    msg+=sprintf "Allocated %% Utilization: The percent of the total *capacity* of the machine that was allocated\n"
    msg+=sprintf "                         by the EMP allocation committee\n"
    msg+=sprintf  "\n"

    return msg

  end


  #####################################################
  #
  # print_user_stats
  #
  #####################################################
  def print_user_stats(userlist=nil)

    msg=sprintf  "\n"
    msg+=sprintf "User Job Summary\n"
    msg+=sprintf "============================================================================================================\n"
    msg+=sprintf "%15s\t%9s\t%9s\t%12s\t%12s\t%12s\t%12s\n","","Total #","Total #","Total Wall","Total CPU","Relative %","Absolute %"
    msg+=sprintf "%15s\t%9s\t%9s\t%12s\t%12s\t%12s\t%12s\n","User","of Jobs","of CPUs","Time (hrs)","Time (hrs)","Utilization","Utilization"
    msg+=sprintf "============================================================================================================\n"
    @user.keys.sort.each { |user|
      unless userlist.nil?
        next if userlist.index(user).nil?
      end
      msg+=sprintf "%15s\t%9d\t%9d\t%12.2f\t%12.2f\t%12.2f\t%12.2f\n",user,
                                                @user[user]["njobs"],
                                                @user[user]["ncpus"],
                                                @user[user]["walltime"]/3600.0,
                                                @user[user]["cputime"]/3600.0,
                                                (@user[user]["cputime"]/3600.0)/(@total["cputime"]/3600.0)*100.0,
                                                (@user[user]["cputime"]/3600.0)/@ncpuhours*100.0
    }
    msg+="\n"

    return msg

  end

  #####################################################
  #
  # print_project_stats
  #
  #####################################################
  def print_project_stats(emplist=nil,projectlist=nil)

    msg=sprintf "\n"
    msg+=sprintf "Project Job Summary\n"
    msg+=sprintf "============================================================================================================\n"
    msg+=sprintf "%15s\t%15s\t%15s\t%9s\t%9s\t%12s\t%12s\n","","","","Total #","Total #","Total Wall","Total CPU"
    msg+=sprintf "%15s\t%15s\t%15s\t%9s\t%9s\t%12s\t%12s\n","EMP","Project","User","of Jobs","of CPUs","Time (hrs)","Time (hrs)"
    msg+=sprintf "============================================================================================================\n"
    @overall_stats.keys.sort.each { |emp|
      unless emplist.nil?
        next if emplist.index(emp).nil?
      end
      msg+=sprintf "%15s\t%15s\t%15s\t%9d\t%9d\t%12.2f\t%12.2f\n",emp,
                                                                    "",
                                                                    "",
                                                                    @emp[emp]["njobs"],
                                                                    @emp[emp]["ncpus"],
                                                                    @emp[emp]["walltime"]/3600.0,
                                                                    @emp[emp]["cputime"]/3600.0
      msg+=sprintf "------------------------------------------------------------------------------------------------------------\n"
      @overall_stats[emp].keys.sort.each { |project|
        unless projectlist.nil?
          next if projectlist.index(project).nil?
        end
        msg+=sprintf "%15s\t%15s\t%15s\t%9d\t%9d\t%12.2f\t%12.2f\n","",
                                                                      project,
                                                                      "",
                                                                      @project[project]["njobs"],
                                                                      @project[project]["ncpus"],
                                                                      @project[project]["walltime"]/3600.0,
                                                                      @project[project]["cputime"]/3600.0
        @overall_stats[emp][project].keys.sort.each { |user|
          msg+=sprintf "%15s\t%15s\t%15s\t%9d\t%9d\t%12.2f\t%12.2f\n","",
                                                                        "",
                                                                        user,
                                                                        @overall_stats[emp][project][user]["njobs"],
                                                                        @overall_stats[emp][project][user]["ncpus"],
                                                                        @overall_stats[emp][project][user]["walltime"]/3600.0,
                                                                        @overall_stats[emp][project][user]["cputime"]/3600.0
        }
      msg+=sprintf "------------------------------------------------------------------------------------------------------------\n"
      }
 
    }
    msg+=sprintf "\n"

    return msg

  end


  #####################################################
  #
  # print_utilization_stats
  #
  #####################################################
  def print_utilization_stats(emplist=nil,projectlist=nil)

    msg=sprintf "\n"
    msg+=sprintf "Project Utilization Summary\n"
    msg+=sprintf "============================================================================================\n"
    msg+=sprintf "%15s\t%15s\t%15s\t%12s\t%12s\t%12s\n","","","","Relative %","Absolute %","Allocated %"
    msg+=sprintf "%15s\t%15s\t%15s\t%12s\t%12s\t%12s\n","EMP","Project","User","Utilization","Utilization","Utilization"
    msg+=sprintf "============================================================================================\n"
    @overall_stats.keys.sort.each { |emp|
      unless emplist.nil?
        next if emplist.index(emp).nil?
      end
      msg+=sprintf "%15s\t%15s\t%15s\t%12.2f\t%12.2f\t%12.2f\n",emp,
                                                                    "",
                                                                    "",
                                                                    (@emp[emp]["cputime"]/3600.0)/(@total["cputime"]/3600.0)*100.0,
                                                                    (@emp[emp]["cputime"]/3600.0)/@ncpuhours*100.0,
                                                                    @emp[emp]["allocation"]
      msg+=sprintf "--------------------------------------------------------------------------------------------\n"
      @overall_stats[emp].keys.sort.each { |project|
        unless projectlist.nil?
          next if projectlist.index(project).nil?
        end
        msg+=sprintf "%15s\t%15s\t%15s\t%12.2f\t%12.2f\n","",
                                                                      project,
                                                                      "",
                                                                      (@project[project]["cputime"]/3600.0)/(@total["cputime"]/3600.0)*100.0,
                                                                      (@project[project]["cputime"]/3600.0)/@ncpuhours*100.0
        @overall_stats[emp][project].keys.sort.each { |user|
          msg+=sprintf "%15s\t%15s\t%15s\t%12.2f\t%12.2f\n","",
                                                                        "",
                                                                        user,
                                                                        (@overall_stats[emp][project][user]["cputime"]/3600.0)/(@total["cputime"]/3600.0)*100.0,
                                                                        (@overall_stats[emp][project][user]["cputime"]/3600.0)/@ncpuhours*100.0
        }
      msg+=sprintf "--------------------------------------------------------------------------------------------\n"
      }
 
    }
    msg+="\n"

    return msg

  end




  #####################################################
  #
  # print_full_report
  #
  #####################################################
  def print_full_report

    puts self.print_header
    puts self.print_summary
    puts self.print_legend
    puts self.print_user_stats
    puts self.print_project_stats
    puts self.print_utilization_stats

  end  

  #####################################################
  #
  # email_emp_reports
  #
  #####################################################
  def email_emp_reports

    @overall_stats.keys.each { |emp|
      report="Subject: EMP Usage Report for #{emp}\n\n"
      report+=self.print_header
      report+=self.print_summary
      report+=self.print_legend
      report+=self.print_project_stats([emp])
      report+=self.print_utilization_stats([emp])

      Net::SMTP.start(@@smtpserver) do |smtp|
        smtp.send_message(report,'jet.mgmt.gsd@noaa.gov',['christopher.w.harrop@noaa.gov'])
      end
    }

  end  

  #####################################################
  #
  # email_project_reports
  #
  #####################################################
  def email_project_reports

    @overall_stats.keys.each { |emp|
      @overall_stats[emp].keys.each { |project|
        report="Subject: Jet Project Usage Report for #{project}\n\n"
        report+=self.print_header
        report+=self.print_summary
        report+=self.print_legend
        report+=self.print_project_stats([emp],[project])
        report+=self.print_utilization_stats([emp],[project])

        Net::SMTP.start(@@smtpserver) do |smtp|
          smtp.send_message(report,'jet.mgmt.gsd@noaa.gov',['christopher.w.harrop@noaa.gov'])
        end

      }
    }

  end  

  

end

$__jetusagereport__ == __FILE__
end
