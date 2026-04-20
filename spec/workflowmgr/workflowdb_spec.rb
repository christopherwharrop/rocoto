# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'workflowmgr/workflowdb'

RSpec.describe WorkflowMgr::WorkflowSQLite3DB do
  describe 'workflow locking' do
    let(:databasefile) { 'test.db' }

    before do
      FileUtils.rm_f(databasefile)
    end

    after do
      FileUtils.rm_f(databasefile)
    end

    it 'properly locks and serializes database access across processes' do
      # Initialize a workflow SQLite database
      database = described_class.new(databasefile)

      # Add a test table to the database
      dbhandle = SQLite3::Database.new(databasefile)
      dbhandle.transaction do |db|
        db.execute('CREATE TABLE test (val INTEGER);')
      end
      dbhandle.close

      # Fork 10 processes, each doing 100 database operations
      pids = []
      10.times do
        pids << Process.fork do
          100.times do
            database.lock_workflow

            # Get a handle to the database
            dbhandle = SQLite3::Database.new(databasefile)

            dbhandle.transaction do |db|
              val = db.execute('SELECT val FROM test')
              if val.empty?
                db.execute('INSERT into test values (1)')
              else
                db.execute("UPDATE test SET val=#{val[0][0] + 1}")
              end
            end

            dbhandle.close
            database.unlock_workflow
          end
          exit 0
        end
      end

      # Wait for all child processes
      ndbaccess = 0
      pids.each do |pid|
        _childpid, status = Process.waitpid2(pid)
        ndbaccess += 1 if status.exitstatus == 0
      end

      # Verify the final value
      dbhandle = SQLite3::Database.new(databasefile)
      dbhandle.transaction do |db|
        val = db.execute('SELECT val FROM test')
        if ndbaccess.positive?
          # If locking works correctly, all 100 operations should have incremented the counter
          expect(val).to eq([[100]])
        else
          # If no child processes succeeded (shouldn't happen), table should be empty
          expect(val).to eq([])
        end
      end
      dbhandle.close
    end
  end
end
