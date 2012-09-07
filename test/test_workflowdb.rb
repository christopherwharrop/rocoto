if RUBY_VERSION < "1.9.0"
  require 'require_relative'
end

# Get the base directory of the WFM installation
if File.symlink?(__FILE__)
  __WFMDIR__=File.dirname(File.dirname(File.expand_path(File.readlink(__FILE__),File.dirname(__FILE__))))
else
  __WFMDIR__=File.dirname(File.expand_path(File.dirname(__FILE__)))
end

# Add include paths for WFM and libxml-ruby libraries
$:.unshift("#{__WFMDIR__}/lib")
$:.unshift("#{__WFMDIR__}/libxml-ruby/lib")
$:.unshift("#{__WFMDIR__}/libxml-ruby/ext/libxml")
$:.unshift("#{__WFMDIR__}/sqlite3-ruby/lib")
$:.unshift("#{__WFMDIR__}/sqlite3-ruby/ext")


require 'test/unit'
require 'fileutils'
require_relative '../lib/workflowmgr/workflowdb'

class TestWorkflowDB < Test::Unit::TestCase

  def test_workflow_locking

    databasefile="test.db" 

    FileUtils.rm_f(databasefile)
   
    # Initialize a workflow SQLite database
    database=WorkflowMgr::WorkflowSQLite3DB.new(databasefile)

    # Add a test table to the database
    dbhandle = SQLite3::Database.new(databasefile)
    dbhandle.transaction do |db|
      db.execute("CREATE TABLE test (val INTEGER);")
    end

    pids=[]
    10.times do
      pids << Process.fork do
        100.times do
          database.lock_workflow

          # Get a handle to the database
          dbhandle = SQLite3::Database.new(databasefile)

          val=[]

          dbhandle.transaction do |db|
            val=db.execute("SELECT val FROM test")
            if val.empty?
              db.execute("INSERT into test values (1)")
            else
              db.execute("UPDATE test SET val=#{val[0][0]+1}")
            end
          end  # database transaction

          puts val.inspect

          database.unlock_workflow
        end
      end
    end

    ndbaccess=0
    pids.each { |pid| 
      childpid,status=Process.waitpid2(pid) 
      ndbaccess+=1 if status.exitstatus==0
    }

    dbhandle.transaction do |db|
      val=db.execute("SELECT val FROM test")
      if ndbaccess > 0
        assert_equal([[100]],val)
      else
	puts val.inspect
        assert_equal([],val)
      end
    end


  end

end

