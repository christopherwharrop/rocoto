# frozen_string_literal: true

require 'spec_helper'
require 'workflowmgr/workflowoption'

RSpec.describe WorkflowMgr::WorkflowOption do
  describe 'workflow path option' do
    it 'parses -w short option' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '-v'])
      expect(options.workflowdoc).to eq('workflowpath')
    end

    it 'parses --workflow long option' do
      options = described_class.new(['--workflow', 'workflowpath', '-d', 'databasepath', '-v'])
      expect(options.workflowdoc).to eq('workflowpath')
    end

    it 'parses --workflow= with equals syntax' do
      options = described_class.new(['--workflow=workflowpath', '-d', 'databasepath', '-v'])
      expect(options.workflowdoc).to eq('workflowpath')
    end
  end

  describe 'database option' do
    it 'parses -d short option' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '-v'])
      expect(options.database).to eq('databasepath')
    end

    it 'parses --database long option' do
      options = described_class.new(['-w', 'workflowpath', '--database', 'databasepath', '-v'])
      expect(options.database).to eq('databasepath')
    end

    it 'parses --database= with equals syntax' do
      options = described_class.new(['-w', 'workflowpath', '--database=databasepath', '-v'])
      expect(options.database).to eq('databasepath')
    end
  end

  describe 'verbose option' do
    it 'defaults to 1 with -v flag' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '-v'])
      expect(options.verbose).to eq(1)
    end

    it 'accepts numeric value with -v' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '-v', '10'])
      expect(options.verbose).to eq(10)
    end

    it 'defaults to 1 with --verbose flag' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '--verbose'])
      expect(options.verbose).to eq(1)
    end

    it 'accepts numeric value with --verbose' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '--verbose', '10'])
      expect(options.verbose).to eq(10)
    end

    it 'accepts numeric value with --verbose=' do
      options = described_class.new(['-w', 'workflowpath', '-d', 'databasepath', '--verbose=10'])
      expect(options.verbose).to eq(10)
    end
  end
end
