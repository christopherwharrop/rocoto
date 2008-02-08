unless defined? $__jetusagereport__

##########################################
#
# Class JetUsageReport
#
##########################################
class JetUsageReport

  if File.symlink?(__FILE__)
    $:.insert($:.size-1,File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__)))
  else
    $:.insert($:.size-1,File.dirname(__FILE__)) << File.dirname(__FILE__) unless $:.include?(File.dirname(__FILE__))
  end

  require 'sgebatchsystem.rb'


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
      when /ecomp/
        @machine="eJET"
      when /wcomp/
        @machine="wJET"
    end

    @stime=stime
    @etime=etime
    @users=users
    @projects=projects

    sge=SGEBatchSystem.new()
    stats=sge.collect_job_stats(@stime,@etime,@users,@projects).split(/%/)
    @total_stats=stats[0].split(/:/)
    @user_stats=stats[1].split("$")
    @project_stats=stats[2].split("$")
    @emp_stats=stats[3].split("$")

  end


  #####################################################
  #
  # print_overall_summary
  #
  #####################################################
  def print_overall_summary

    puts
    puts "Job Summary"
    puts "--------------------------------------------"
    printf "Total Number of Jobs Completed: %12d\n",@total_stats[0]
    printf "     Total Number of CPUs Used: %12d\n",@total_stats[1]
    printf "   Total Wall Clock Time (hrs): %12.2f\n",@total_stats[2].to_i/3600.0
    printf "          Total CPU Time (hrs): %12.2f\n",@total_stats[3].to_i/3600.0
    puts
   
  end

  #####################################################
  #
  # print_stats
  #
  #####################################################
  def print_category_stats(header,category,stats)

    puts
    puts header
    puts "-----------------------------------------------------------------------------"
    printf "%15s\t%9s\t%9s\t%12s\t%12s\n","","Total #","Total #","Total Wall","Total CPU"
    printf "%15s\t%9s\t%9s\t%12s\t%12s\n",category,"of Jobs","of CPUs","Time (hrs)","Time (hrs)"
    puts "-----------------------------------------------------------------------------"    
    stats.each { |record|
      cat,njobs,cpus,wallh,cpuh=record.split(":")
      printf "%15s\t%9d\t%9d\t%12.2f\t%12.2f\n",cat,njobs,cpus,wallh.to_i/3600.0,cpuh.to_i/3600.0
    }    
    puts

  end



  #####################################################
  #
  # print_header
  #
  #####################################################
  def print_header

    # Convert start and end time strings to Time objects
    stime_arr=@stime.gsub(/[-_:]/,":").split(":")
    etime_arr=@etime.gsub(/[-_:]/,":").split(":")

    puts
    puts "#{@machine} Usage Report For: #{Time.gm(*stime_arr).strftime("%m/%d/%Y %H:%M:%S %Z")} thru #{Time.gm(*etime_arr).strftime("%m/%d/%Y %H:%M:%S %Z")}"
    puts

  end


  #####################################################
  #
  # print_user_stats
  #
  #####################################################
  def print_user_stats

    self.print_category_stats("User Job Summary","User",@user_stats)   

  end  


  #####################################################
  #
  # print_project_stats
  #
  #####################################################
  def print_project_stats

    self.print_category_stats("Project Job Summary","Project",@project_stats)   

  end  

  #####################################################
  #
  # print_emp_stats
  #
  #####################################################
  def print_emp_stats

    self.print_category_stats("EMP Job Summary","EMP",@emp_stats)   

  end  


  #####################################################
  #
  # print_full_report
  #
  #####################################################
  def print_full_report

    self.print_header
    self.print_overall_summary
    self.print_user_stats
    self.print_project_stats
    self.print_emp_stats

  end  
  


  #####################################################
  #
  # print_node_hist
  #
  #####################################################
  def print_node_hist(bins)
    
    node_hist=Array.new
    node_hist.fill(0,0,bins.length+1)

    @jobs.each_value { |val|
      nodes=val[3].to_i
      index=0
      bins.each { |bin|
        if nodes > bin
          index+=1
        else
          break
        end
      }
      node_hist[index]+=1
    }
    puts
    printf "%16s","CPUs:";
    bins.each { |bin|
      if (bin.class==Fixnum) 
        printf "%8d",bin
      else
	printf "%8.2f",bin
      end
    }
    printf "%8s","Inf"
    puts
    printf "------------------------"
    bins.each { |bin|
        printf "--------"
    }
    puts
    printf "%16s","Count:"
    node_hist.each { |hist|
      printf "%8d",hist
    }
    puts
    printf "%16s","Percent:"
    cnodes=0.0
    node_hist.each { |hist|
      cnodes+=hist
      if (@jobs.size != 0)
	printf "%8.2f",cnodes/@jobs.size
      else
	printf "%8.2f",0.0
      end
    }
    puts

  end

  #####################################################
  #
  # print_wallh_hist
  #
  #####################################################
  def print_wallh_hist(bins)
    
    wallh_hist=Array.new
    wallh_hist.fill(0,0,bins.length+1)

    @jobs.each_value { |val|
      wallh=val[2].to_i/3600.0
      index=0
      bins.each { |bin|
        if wallh > bin
          index+=1
        else
          break
        end
      }
      wallh_hist[index]+=1
    }
    puts
    printf "%16s","Wall Time (hrs):";
    bins.each { |bin|
      if (bin.class==Fixnum) 
        printf "%8d",bin
      else
	printf "%8.2f",bin
      end
    }
    printf "%8s","Inf"
    puts
    printf "------------------------"
    bins.each { |bin|
        printf "--------"
    }
    puts
    printf "%16s","Count:"
    wallh_hist.each { |hist|
      printf "%8d",hist
    }
    puts
    printf "%16s","Percent:"
    cnodes=0.0
    wallh_hist.each { |hist|
      cnodes+=hist
      if (@jobs.size != 0)
	printf "%8.2f",cnodes/@jobs.size
      else
	printf "%8.2f",0.0
      end
    }
    puts

  end

  #####################################################
  #
  # print_cpuh_hist
  #
  #####################################################
  def print_cpuh_hist(bins)
    
    cpuh_hist=Array.new
    cpuh_hist.fill(0,0,bins.length+1)

    @jobs.each_value { |val|
      cpuh=val[2].to_i/3600.0 * val[3].to_i
      index=0
      bins.each { |bin|
        if cpuh > bin
          index+=1
        else
          break
        end
      }
      cpuh_hist[index]+=1
    }
    puts
    printf "%16s","CPU Time (hrs):";
    bins.each { |bin|
      if (bin.class==Fixnum) 
        printf "%8d",bin
      else
	printf "%8.2f",bin
      end
    }
    printf "%8s","Inf"
    puts
    printf "------------------------"
    bins.each { |bin|
        printf "--------"
    }
    puts
    printf "%16s","Count:"
    cpuh_hist.each { |hist|
      printf "%8d",hist
    }
    puts
    printf "%16s","Percent:"
    cnodes=0.0
    cpuh_hist.each { |hist|
      cnodes+=hist
      if (@jobs.size != 0)
	printf "%8.2f",cnodes/@jobs.size
      else
	printf "%8.2f",0.0
      end
    }
    puts

  end

end

$__jetusagereport__ == __FILE__
end
