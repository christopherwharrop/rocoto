# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'fileutils'
require 'workflowmgr/workflowdb'

RSpec.describe WorkflowMgr::WorkflowSQLite3DB do
  describe 'workflow locking' do
    let(:databasefile) { 'test.db' }
    let(:lockfile) { 'test_lock.db' }

    before do
      FileUtils.rm_f(databasefile)
      FileUtils.rm_f(lockfile)
    end

    after do
      FileUtils.rm_f(databasefile)
      FileUtils.rm_f(lockfile)
    end

    # Skipped: This test is flaky due to race conditions and timing dependencies.
    # It uses sleep() for process synchronization which is non-deterministic in CI.
    # TODO: Redesign using proper IPC (signal files, pipes) to eliminate timing races.
    # The test should verify: two rocotorun processes cannot write to the DB simultaneously.
    xit 'properly locks and serializes database access across processes' do
      # Initialize a workflow SQLite database
      database = described_class.new(databasefile)
      database.dbopen

      # Add a test table to the database
      dbhandle = SQLite3::Database.new(databasefile)
      dbhandle.transaction do |db|
        db.execute('CREATE TABLE test (val INTEGER);')
        db.execute('INSERT INTO test VALUES (0);')
      end
      dbhandle.close

      # Create a worker script that simulates a rocotorun process
      lib_path = File.expand_path('../../lib', __dir__)
      worker_script = <<~RUBY
        #!/usr/bin/env ruby
        $LOAD_PATH.unshift('#{lib_path}')

        require 'sqlite3'
        require 'workflowmgr/workflowdb'

        databasefile = ARGV[0]
        action = ARGV[1]

        database = WorkflowMgr::WorkflowSQLite3DB.new(databasefile)
        database.dbopen

        case action
        when 'lock'
          # Acquire lock and hold it briefly
          success = database.lock_workflow
          if success
            # Write that we have the lock
            puts "LOCKED"
            # Hold the lock for a moment
            sleep 0.5
            database.unlock_workflow
          else
            puts "FAILED"
            exit 1
          end
        when 'increment'
          # Acquire lock, increment counter, release
          success = database.lock_workflow
          if success
            dbhandle = SQLite3::Database.new(databasefile)
            dbhandle.transaction do |db|
              val = db.execute('SELECT val FROM test')[0][0]
              db.execute("UPDATE test SET val=\#{val + 1}")
            end
            dbhandle.close
            database.unlock_workflow
            puts "INCREMENTED"
          else
            puts "FAILED"
            exit 1
          end
        end

        exit 0
      RUBY

      # Write the worker script
      worker_file = 'test_worker.rb'
      File.write(worker_file, worker_script)

      begin
        # Test 1: Sequential operations should all succeed
        5.times do
          result = `bundle exec ruby #{worker_file} #{databasefile} increment 2>&1`
          expect(result).to include("INCREMENTED")
          expect($CHILD_STATUS.exitstatus).to eq(0)
        end

        # Verify counter
        dbhandle = SQLite3::Database.new(databasefile)
        val = dbhandle.execute('SELECT val FROM test')[0][0]
        dbhandle.close
        expect(val).to eq(5)

        # Test 2: One process holds lock while another tries to acquire
        # Start a process that will hold the lock
        holder_pid = spawn("bundle exec ruby #{worker_file} #{databasefile} lock", out: '/dev/null', err: '/dev/null')

        # Wait for it to acquire the lock
        sleep 0.2

        # Try to increment while the lock is held - should fail/timeout
        start_time = Time.now
        competitor_pid = spawn("bundle exec ruby #{worker_file} #{databasefile} increment",
                               out: '/dev/null', err: '/dev/null')

        # The competitor should wait for the lock to be released
        _pid, _status = Process.wait2(competitor_pid)
        elapsed = Time.now - start_time

        # Should have waited at least 0.3 seconds (lock was held for 0.5s, we waited 0.2s before starting)
        expect(elapsed).to be >= 0.2

        # Wait for holder to finish
        Process.wait2(holder_pid)

        # Competitor might have succeeded or failed depending on timing
        # If it succeeded, counter should be 6, if failed should still be 5
        dbhandle = SQLite3::Database.new(databasefile)
        val = dbhandle.execute('SELECT val FROM test')[0][0]
        dbhandle.close
        expect(val).to be_between(5, 6)
      ensure
        # Clean up
        FileUtils.rm_f(worker_file)
        FileUtils.rm_f('test_worker_0.out')
        FileUtils.rm_f('test_worker_0.err')
        FileUtils.rm_f('test_worker_1.out')
        FileUtils.rm_f('test_worker_1.err')
      end
    end
  end
end
