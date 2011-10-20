##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowLockedException
  #
  ##########################################
  class WorkflowLockedException < RuntimeError
  end

  ##########################################
  #
  # Class WorkflowSQLite3DB
  #
  ##########################################
  class WorkflowSQLite3DB

    require 'sqlite3'
    require "socket"
    require 'workflowmgr/forkit'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(database_file)

      @database_file=database_file

      begin

        # Fork a process to access the database and initialize it
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get a listing of the database tables
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

            # Create all tables that are missing from the database 
            create_tables(db,tables.flatten)

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1
      end  # begin

    end  # initialize


    ##########################################
    #
    # lock_workflow
    #
    ##########################################
    def lock_workflow

      begin

        # Fork a process to access the database and acquire the workflow lock
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Access the workflow lock maintained by the workflow manager
            lock=db.execute("SELECT * FROM lock;")      

            # If no lock is present, we have acquired the lock.  Write the WFM's pid and host into the lock table
            if lock.empty?
              db.execute("INSERT INTO lock VALUES (#{Process.ppid},'#{Socket.gethostname}',#{Time.now.to_i});") 
            else
	      raise WorkflowMgr::WorkflowLockedException, "ERROR: Workflow is locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}"
            end

          end  # database transaction

        end  # forkit

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1
      end  # begin

    end


    ##########################################
    #
    # unlock_workflow
    #
    ##########################################
    def unlock_workflow

      begin

        # Open the database and initialize it if necessary
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            lock=db.execute("SELECT * FROM lock;")      

            if Process.ppid==lock[0][0] && Socket.gethostname==lock[0][1]
              db.execute("DELETE FROM lock;")      
            else
              raise WorkflowMgr::WorkflowLockedException, "ERROR: Process #{Process.ppid} cannot unlock the workflow because it does not own the lock." +
                                                          "       The workflow is already locked by pid #{lock[0][0]} on host #{lock[0][1]} since #{Time.at(lock[0][2])}."
            end

          end  # database transaction

        end  # forkit

      rescue WorkflowMgr::WorkflowLockedException => e
        STDERR.puts e.message
        exit 1
      rescue SQLite3::BusyException
        STDERR.puts 
        STDERR.puts "ERROR: Could not unlock the workflow.  The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        exit -1

      end  # begin

    end


    ##########################################
    #
    # get_cyclespecs
    #
    ##########################################
    def get_cyclespecs

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          dbspecs=[]

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Access the workflow lock maintained by the workflow manager
            dbspecs=db.execute("SELECT groupname,fieldstr,dirty FROM cyclespec;")

          end  # database transaction
          
          # Return the array of cycle specs
          dbspecs.collect { |spec| {:group=>spec[0], :fieldstr=>spec[1], :dirty=>spec[2]} }

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # update_cyclespecs
    #
    ##########################################
    def update_cyclespecs(newspecs)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Retreive the current cycle specs from the database
            dbspecs=db.execute("SELECT groupname,fieldstr FROM cyclespec;")
 
            # Delete cycles specs from the database that are not in the new cycle spec list
            # Update dirty flag for cycle specs in the new cycle spec list that are already in the database
            dbspecs.each do |dbspec|
              
              # Find the matching new cycle spec, if there is one, for this database cycle spec
              newspec=newspecs.find { |newspec| newspec[:group]==dbspec[0] && newspec[:fieldstr]==dbspec[1] }

              # If no such cycle spec was found, delete the cycle spec from the database
              if newspec.nil?
                db.execute("DELETE FROM cyclespec WHERE groupname='#{dbspec[0]}' and fieldstr='#{dbspec[1]}';")

              # Otherwise, update the :dirty field of the cycle spec in the database
              else               
                unless newspec[:dirty].nil?
                  db.execute("UPDATE cyclespec SET dirty=#{newspec[:dirty]} WHERE groupname='#{newspec[:group]}' and fieldstr='#{newspec[:fieldstr]}';")
                end
              end

            end

            # Add incoming cycles that are not in database and initialize them as dirty
            newspecs.each do |spec|
              unless dbspecs.member?([spec[:group],spec[:fieldstr]])
                db.execute("INSERT INTO cyclespec VALUES (NULL,'#{spec[:group]}','#{spec[:fieldstr]}',1);")
              end
            end

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin
  
    end


    ##########################################
    #
    # get_all_cycles
    #
    ##########################################
    def get_all_cycles

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          dbcycles=[]

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Retrieve all cycles from the cycle table
            dbcycles=db.execute("SELECT cycle,activated,done FROM cycles;")

          end  # database transaction
          
          # Return an array of cycles
          dbcycles.collect { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :done=>Time.at(cycle[2]).getgm} }

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # get_last_cycle
    #
    ##########################################
    def get_last_cycle

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          dbcycles=[]

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get the maximum cycle time from the database
            max_cycle=db.execute("SELECT MAX(cycle) FROM cycles")[0][0]
            unless max_cycle.nil?
              dbcycles=db.execute("SELECT cycle,activated,done FROM cycles WHERE cycle=#{max_cycle}")
            end

          end  # database transaction
          
          # Return the last cycle
          dbcycles.collect { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :done=>Time.at(cycle[2]).getgm} }.first

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end

    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles(cycle_lifespan=nil)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          dbcycles=[]

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get the cycles that are neither done nor expired
            if cycle_lifespan.nil?
              dbcycles=db.execute("SELECT cycle,activated,done FROM cycles WHERE done=0;")
            else
              dbcycles=db.execute("SELECT cycle,activated,done FROM cycles WHERE done=0 AND activated >= #{Time.now.to_i - cycle_lifespan};")
            end

          end  # database transaction
          
          # Return the array of cycle specs
          dbcycles.collect { |cycle| {:cycle=>Time.at(cycle[0]).getgm, :activated=>Time.at(cycle[1]).getgm, :done=>Time.at(cycle[2]).getgm} }

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end

    ##########################################
    #
    # update_cycles
    #
    ##########################################
    def update_cycles(cycles)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Update each cycle in the database
            cycles.each { |newcycle|
              db.execute("UPDATE cycles SET activated=#{newcycle[:activated].to_i},done=#{newcycle[:done].to_i} WHERE cycle=#{newcycle[:cycle].to_i};")
            }

          end  # database transaction
          
        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # add_cycles
    #
    ##########################################
    def add_cycles(cycles)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Add each cycle to the database
            cycles.each { |newcycle|
              db.execute("INSERT INTO cycles VALUES (NULL,#{newcycle.to_i},#{Time.now.to_i},0);")
            }

          end  # database transaction
          
        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts 
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # get_jobs
    #
    ##########################################
    def get_jobs

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          jobs={}
          dbjobs=[]

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Retrieve all jobs from the cycle table
            dbjobs=db.execute("SELECT jobid,taskid,cycle,state,exit_status,tries FROM jobs;")

          end  # database transaction

          dbjobs.each do |job|
            jobid=job[0]
            jobtask=job[1]
            jobcycle=Time.at(job[2]).getgm
            jobstate=job[3]
            jobstatus=job[4].to_i
            jobtries=job[5].to_i
            jobs[jobtask]={} if jobs[jobtask].nil?
            jobs[jobtask][jobcycle]={} if jobs[jobtask][jobcycle].nil?
            jobs[jobtask][jobcycle][:jobid]=jobid
            jobs[jobtask][jobcycle][:taskid]=jobtask
            jobs[jobtask][jobcycle][:cycle]=jobcycle
            jobs[jobtask][jobcycle][:state]=jobstate
            jobs[jobtask][jobcycle][:exit_status]=jobstatus
            jobs[jobtask][jobcycle][:tries]=jobtries
          end

          # Return jobs hash
          jobs

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # add_jobs
    #
    ##########################################
    def add_jobs(jobs)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Add or update each job in the database
            jobs.each do |job|
              db.execute("INSERT INTO jobs VALUES (NULL,'#{job[:jobid]}','#{job[:taskid]}',#{job[:cycle].to_i},'#{job[:state]}',#{job[:exit_status]},#{job[:tries]});")
            end

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end



    ##########################################
    #
    # update_jobs
    #
    ##########################################
    def update_jobs(jobs)

      begin

        # Fork a process to access the database to retrieve the cyclespecs
        WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Add or update each job in the database
            jobs.each do |job|
              db.execute("UPDATE jobs SET jobid='#{job[:jobid]}',state='#{job[:state]}',exit_status=#{job[:exit_status]},tries=#{job[:tries]} WHERE cycle=#{job[:cycle].to_i} AND taskid='#{job[:taskid]}';")
            end

          end  # database transaction

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil
      end  # begin

    end


    ##########################################
    #
    # get_tables
    #
    ##########################################
    def get_tables(tablelist=nil)

      begin

        tables={}

        # Fork a process to access the database to retrieve the cyclespecs
        cyclespecs=WorkflowMgr.forkit(2) do

          # Get a handle to the database
          database = SQLite3::Database.new(@database_file)

          # Start a transaction so that the database will be locked
          database.transaction do |db|

            # Get a listing of the database tables
            dbtables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            dbtables.flatten!

            # Create an array of rows for each table
            dbtables.each do |table|

              # Initialize the array of rows to be empty
              tables[table.to_sym]=[]

              # Get all rows for the table, where first row is column names
              dbtable=db.execute2("SELECT * FROM #{table};")

              # Get the table column names
              columns=dbtable.shift

              # Add each table row to the array of rows for this table
              dbtable.each do |row|
                rowdata={}

                # Loop over columns, creating a hash for this row's data
                columns.each_with_index do |column,idx|
                  rowdata[column.to_sym]=row[idx]
                end

                # Add the hash representing this row to the array of rows for this table
                tables[table.to_sym] << rowdata

              end 

            end  # dbtables.each

          end  # database transaction

          # Return the tables
          tables

        end  # forkit

      rescue SQLite3::BusyException => e
        STDERR.puts
        STDERR.puts "ERROR: Could not open workflow database file '#{@database_file}'"
        STDERR.puts "       The database is locked by SQLite."
        STDERR.puts
        exit 1
      rescue WorkflowMgr::ForkitTimeoutException
        WorkflowMgr.ioerr(@database_file)
        return nil

      end # begin

    end


  private

    ##########################################
    #
    # create_tables
    #
    ##########################################
    def create_tables(db,tables)

      raise "WorkflowSQLite3DB::create_tables must be called inside a transaction" unless db.transaction_active?

      # Create the lock table
      unless tables.member?("lock")
        db.execute("CREATE TABLE lock (pid INTEGER, host VARCHAR(64), time DATETIME);")
      end

      # Create the cyclespec table
      unless tables.member?("cyclespec")
        db.execute("CREATE TABLE cyclespec (id INTEGER PRIMARY KEY, groupname VARCHAR(64), fieldstr VARCHAR(256), dirty BOOLEAN);")
      end

      # Create the cycles table
      unless tables.member?("cycles")
        db.execute("CREATE TABLE cycles (id INTEGER PRIMARY KEY, cycle DATETIME, activated DATETIME, done DATETIME);")
     end

     # Create the jobs table
      unless tables.member?("jobs")
        db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, jobid VARCHAR(64), taskid VARCHAR(64), cycle DATETIME, state VARCHAR[64], exit_status INTEGER, tries INTEGER);")
     end

    end  # create_tables

  end  # Class WorkflowSQLite3DB

end  # Module WorkflowMgr
