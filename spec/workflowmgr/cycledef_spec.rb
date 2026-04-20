# frozen_string_literal: true

require 'spec_helper'
require 'fileutils'
require 'workflowmgr/cycledef'

RSpec.describe WorkflowMgr::CycleCron do
  describe '#initialize' do
    context 'with various cron field formats' do
      let(:testfields) do
        [
          # Asterisk forms
          '* * * * * *',
          '*/2 * * * * *',
          # Non-asterisk forms
          '0 * * * * *',
          '0-10 * * * * *',
          '0-10/2 * * * * *',
          # Lists following single integer
          '0,1 * * * * *',
          '0,1-10 * * * * *',
          '0,1-10/2 * * * * *',
          # Lists following range
          '0-10,11 * * * * *',
          '0-10,11-20 * * * * *',
          '0-10,11-20/2 * * * * *',
          # Lists following stepped range
          '0-10/2,11 * * * * *',
          '0-10/2,11-20 * * * * *',
          '0-10/2,11-20/2 * * * * *'
        ]
      end

      it 'parses all cron field format variations' do
        testfields.each do |testfield|
          cycle = described_class.new(testfield, 'test', 0)
          expect(cycle.cycledef).to eq(testfield)
          expect(cycle.group).to eq('test')
        end
      end
    end
  end

  describe '#first' do
    it 'returns epoch for wildcard pattern' do
      cycle = described_class.new('* * * * * *', 'test', 0)
      expect(cycle.first).to eq(Time.gm(1900, 1, 1, 0, 0))
    end

    it 'returns first cycle for year-limited pattern' do
      cycle = described_class.new('0 */6 * * 2008-2012 *', 'test', 0)
      expect(cycle.first).to eq(Time.gm(2008, 1, 1, 0, 0))
    end

    it 'returns first cycle for specific month and day range' do
      cycle = described_class.new('30 12 15-31 4,8 2010 *', 'test', 0)
      expect(cycle.first).to eq(Time.gm(2010, 4, 15, 12, 30))
    end
  end

  describe '#next' do
    it 'returns current time when called with now for wildcard pattern' do
      cycle = described_class.new('* * * * * *', 'test', 0)
      reftime = Time.at(Time.now.to_i)
      reftime -= reftime.sec
      nextcycle = cycle.next(reftime)
      expect(nextcycle[0]).to eq(reftime)
    end

    context 'with specific hour pattern' do
      let(:cycle) { described_class.new('0 0 * * * *', 'test', 0) }

      it 'returns midnight on Jan 1 when called with Jan 1 midnight' do
        reftime = Time.gm(2011, 1, 1, 0, 0)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end

      it 'returns next day midnight when called with previous day evening' do
        reftime = Time.gm(2010, 12, 31, 18, 0)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end

      it 'handles leap year correctly' do
        reftime = Time.gm(2008, 2, 28, 18, 0)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2008, 2, 29, 0, 0))
      end
    end

    context 'with multiple hours pattern' do
      let(:cycle) { described_class.new('0 0,12 * * * *', 'test', 0) }

      it 'finds next 12-hour cycle correctly' do
        reftime = Time.gm(2009, 2, 28, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2009, 3, 1, 0, 0))
      end
    end

    context 'with day-of-month restrictions' do
      let(:cycle) { described_class.new('0 0,12 15-31 * * *', 'test', 0) }

      it 'returns same day when within range' do
        reftime = Time.gm(2009, 12, 31, 11, 23)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2009, 12, 31, 12, 0))
      end

      it 'skips to next valid day when past last hour' do
        reftime = Time.gm(2009, 12, 31, 18, 23)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2010, 1, 15, 0, 0))
      end
    end

    context 'with 31st day pattern' do
      let(:cycle) { described_class.new('0 0,12 31 * * *', 'test', 0) }

      it 'skips months without 31 days' do
        reftime = Time.gm(2009, 3, 31, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2009, 5, 31, 0, 0))
      end

      it 'skips from April to May' do
        reftime = Time.gm(2009, 4, 28, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2009, 5, 31, 0, 0))
      end
    end

    context 'with month restrictions' do
      let(:cycle) { described_class.new('0 12 * 6-8 * *', 'test', 0) }

      it 'wraps to next year when past last month' do
        reftime = Time.gm(2008, 8, 31, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2009, 6, 1, 12, 0))
      end
    end

    # context 'with invalid month/day combinations' do
    #   let(:cycle) { described_class.new('0 0 31 9 * *', 'test', 0) }

    #   it 'handles September 31st correctly' do
    #     reftime = Time.gm(2009, 9, 30, 15, 43)
    #     nextcycle = cycle.next(reftime)
    #     expect(nextcycle[0]).to eq(Time.gm(2009, 10, 1, 0, 0))
    #   end
    # end

    context 'with leap year constraints' do
      let(:cycle) { described_class.new('0 0,12 29-31 2 * *', 'test', 0) }

      it 'finds next leap year Feb 29' do
        reftime = Time.gm(2009, 3, 28, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2012, 2, 29, 0, 0))
      end
    end

    context 'with day-of-week constraints' do
      it 'handles Monday constraint correctly' do
        cycle = described_class.new('0 0,12 3-31 2 * 1', 'test', 0)
        reftime = Time.gm(2009, 2, 28, 15, 43) # Saturday
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2010, 2, 1, 0, 0))
      end

      it 'handles Saturday constraint correctly' do
        cycle = described_class.new('0 0,12 3-31 2 * 6', 'test', 0)
        reftime = Time.gm(2009, 2, 28, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle[0]).to eq(Time.gm(2010, 2, 3, 0, 0))
      end
    end

    context 'with year constraints' do
      it 'returns nil when past end year' do
        cycle = described_class.new('0 12 * 6-8 2010 *', 'test', 0)
        reftime = Time.gm(2010, 8, 31, 15, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle).to be_nil
      end

      it 'returns nil with all hours past end year' do
        cycle = described_class.new('0 * * 6-8 2010 *', 'test', 0)
        reftime = Time.gm(2010, 8, 31, 23, 43)
        nextcycle = cycle.next(reftime)
        expect(nextcycle).to be_nil
      end
    end
  end

  describe '#previous' do
    it 'returns current time when called with now for wildcard pattern' do
      cycle = described_class.new('* * * * * *', 'test', 0)
      reftime = Time.at(Time.now.to_i)
      reftime -= reftime.sec
      prevcycle = cycle.previous(reftime)
      expect(prevcycle[0]).to eq(reftime)
    end

    it 'returns same cycle on exact match' do
      cycle = described_class.new('0 0 * * * *', 'test', 0)
      reftime = Time.gm(2011, 1, 1, 0, 0)
      prevcycle = cycle.previous(reftime)
      expect(prevcycle[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
    end

    it 'handles leap year day correctly' do
      cycle = described_class.new('0 12 * * * *', 'test', 0)
      reftime = Time.gm(2008, 3, 1, 0, 0)
      prevcycle = cycle.previous(reftime)
      expect(prevcycle[0]).to eq(Time.gm(2008, 2, 29, 12, 0))
    end

    it 'handles non-leap year correctly' do
      cycle = described_class.new('0 12 * * * *', 'test', 0)
      reftime = Time.gm(2011, 3, 1, 0, 0)
      prevcycle = cycle.previous(reftime)
      expect(prevcycle[0]).to eq(Time.gm(2011, 2, 28, 12, 0))
    end

    it 'handles day-of-week constraints' do
      cycle = described_class.new('0 12 3-31 2 * 1', 'test', 0)
      reftime = Time.gm(2010, 2, 1, 0, 0)
      prevcycle = cycle.previous(reftime)
      expect(prevcycle[0]).to eq(Time.gm(2009, 2, 28, 12, 0))
    end
  end

  describe '#member?' do
    it 'returns true for any time with wildcard pattern' do
      cycle = described_class.new('* * * * * *', 'test', 0)
      expect(cycle.member?(Time.now)).to be true
    end

    it 'returns false for times outside year range' do
      cycle = described_class.new('* * * * 1970 *', 'test', 0)
      expect(cycle.member?(Time.now)).to be false
    end

    context 'with day-of-week constraints' do
      let(:cycle) { described_class.new('* * * * 2011 2-5', 'test', 0) }

      it 'returns true for Tuesday (day 2)' do
        expect(cycle.member?(Time.gm(2011, 8, 30))).to be true
      end

      it 'returns false for Monday (day 1)' do
        expect(cycle.member?(Time.gm(2011, 8, 29))).to be false
      end
    end

    context 'with both day-of-month and day-of-week constraints' do
      let(:cycle) { described_class.new('* * 10-15 * 2011 2-5', 'test', 0) }

      it 'returns true for day in range' do
        expect(cycle.member?(Time.gm(2011, 8, 15))).to be true
      end

      it 'returns true for valid day-of-week outside range' do
        expect(cycle.member?(Time.gm(2011, 8, 30))).to be true
      end

      it 'returns false for day outside range and invalid day-of-week' do
        expect(cycle.member?(Time.gm(2011, 8, 1))).to be false
      end
    end
  end
end

RSpec.describe WorkflowMgr::CycleInterval do
  describe '#initialize' do
    it 'accepts DD:HH:MM:SS format' do
      expect do
        described_class.new('201101010000 201201010000 1:00:00:00', 'test')
      end.not_to raise_error
    end

    it 'accepts HH:MM:SS format' do
      expect do
        described_class.new('201101010000 201201010000 1:00:00', 'test')
      end.not_to raise_error
    end

    it 'accepts MM:SS format' do
      expect do
        described_class.new('201101010000 201201010000 1:00', 'test')
      end.not_to raise_error
    end
  end

  describe '#first' do
    it 'returns start time for DD:HH:MM:SS format' do
      cycle = described_class.new('201101010000 201201010000 1:00:00:00', 'test')
      expect(cycle.first).to eq(Time.gm(2011, 1, 1, 0))
    end

    it 'returns start time for HH:MM:SS format' do
      cycle = described_class.new('201101010000 201201010000 1:00:00', 'test')
      expect(cycle.first).to eq(Time.gm(2011, 1, 1, 0))
    end

    it 'returns start time for MM:SS format' do
      cycle = described_class.new('201101010000 201201010000 1:00', 'test')
      expect(cycle.first).to eq(Time.gm(2011, 1, 1, 0))
    end
  end

  describe '#next' do
    context 'with daily interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00:00:00', 'test') }

      it 'returns first cycle when called with first time' do
        result = cycle.next(cycle.first)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0))
      end

      it 'returns next day when called after first' do
        result = cycle.next(cycle.first + 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 2, 0))
      end

      it 'returns first cycle when called before first' do
        result = cycle.next(cycle.first - 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0))
      end
    end

    context 'with hourly interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00:00', 'test') }

      it 'returns first cycle when called with first time' do
        result = cycle.next(cycle.first)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0))
      end

      it 'returns next hour when called after first' do
        result = cycle.next(cycle.first + 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 1))
      end

      it 'returns first cycle when called before first' do
        result = cycle.next(cycle.first - 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0))
      end
    end

    context 'with minute interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00', 'test') }

      it 'returns first cycle when called with first time' do
        result = cycle.next(cycle.first)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end

      it 'returns next minute when called after first' do
        result = cycle.next(cycle.first + 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0, 1))
      end

      it 'returns first cycle when called before first' do
        result = cycle.next(cycle.first - 1)
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end
    end
  end

  describe '#previous' do
    context 'with daily interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00:00:00', 'test') }

      it 'returns exact cycle on exact match' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0))
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0))
      end

      it 'returns same cycle when called slightly after' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) + 1)
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0))
      end

      it 'returns previous day when called slightly before' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) - 1)
        expect(result[0]).to eq(Time.gm(2011, 2, 28, 0))
      end
    end

    context 'with hourly interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00:00', 'test') }

      it 'returns exact cycle on exact match' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0))
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0))
      end

      it 'returns same cycle when called slightly after' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) + 1)
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0))
      end

      it 'returns previous hour when called slightly before' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) - 1)
        expect(result[0]).to eq(Time.gm(2011, 2, 28, 23))
      end
    end

    context 'with minute interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 1:00', 'test') }

      it 'returns exact cycle on exact match' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0))
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0, 0))
      end

      it 'returns same cycle when called slightly after' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) + 1)
        expect(result[0]).to eq(Time.gm(2011, 3, 1, 0, 0))
      end

      it 'returns previous minute when called slightly before' do
        result = cycle.previous(Time.gm(2011, 3, 1, 0, 0, 0) - 1)
        expect(result[0]).to eq(Time.gm(2011, 2, 28, 23, 59))
      end
    end
  end

  describe '#member?' do
    let(:cycle) { described_class.new('201101010000 201201010000 1:00:00:00', 'test') }

    it 'returns false for times before range' do
      expect(cycle.member?(Time.gm(1970, 1, 1))).to be false
    end

    it 'returns false for times after range' do
      expect(cycle.member?(Time.gm(2100, 1, 1))).to be false
    end

    it 'returns true for times within range' do
      expect(cycle.member?(Time.gm(2011, 8, 1))).to be true
    end

    context 'with 2-day interval' do
      let(:cycle) { described_class.new('201101010000 201201010000 2:00:00:00', 'test') }

      it 'returns false for day 2 (not on interval)' do
        expect(cycle.member?(Time.gm(2011, 1, 2))).to be false
      end

      it 'returns true for day 3 (on interval)' do
        expect(cycle.member?(Time.gm(2011, 1, 3))).to be true
      end
    end
  end

  describe 'exclude_hours attribute' do
    let(:cycle) do
      described_class.new('201101010000 201101020600 06:00:00', 'test', 0, nil, '6 18')
    end

    describe '#next' do
      it 'includes hour 0 when hour 6 and 18 are excluded' do
        result = cycle.next(Time.gm(2011, 1, 1, 0, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end

      it 'skips hour 6 and goes to hour 12' do
        result = cycle.next(Time.gm(2011, 1, 1, 0, 1))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 12, 0))
      end

      it 'skips hour 18 and goes to next day hour 0' do
        result = cycle.next(Time.gm(2011, 1, 1, 12, 1))
        expect(result[0]).to eq(Time.gm(2011, 1, 2, 0, 0))
      end
    end

    describe '#previous' do
      it 'returns hour 12 when called from hour 18 (excluded)' do
        result = cycle.previous(Time.gm(2011, 1, 1, 18, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 12, 0))
      end

      it 'returns hour 12 when called from hour 12' do
        result = cycle.previous(Time.gm(2011, 1, 1, 12, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 12, 0))
      end

      it 'returns hour 0 when called from hour 6 (excluded)' do
        result = cycle.previous(Time.gm(2011, 1, 1, 6, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 0, 0))
      end
    end

    describe '#member?' do
      it 'includes hour 0' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 0, 0))).to be true
      end

      it 'excludes hour 6' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 6, 0))).to be false
      end

      it 'includes hour 12' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 12, 0))).to be true
      end

      it 'excludes hour 18' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 18, 0))).to be false
      end
    end

    describe '#first' do
      it 'returns hour 0' do
        expect(cycle.first).to eq(Time.gm(2011, 1, 1, 0, 0))
      end
    end
  end

  describe 'valid_hours attribute' do
    let(:cycle) do
      described_class.new('201101010000 201101020600 01:00:00', 'test', 0, nil, nil, '3 9 15')
    end

    describe '#next' do
      it 'finds first valid hour from hour 0' do
        result = cycle.next(Time.gm(2011, 1, 1, 0, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 3, 0))
      end

      it 'progresses from hour 3 to 9' do
        result = cycle.next(Time.gm(2011, 1, 1, 3, 1))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 9, 0))
      end

      it 'progresses from hour 9 to 15' do
        result = cycle.next(Time.gm(2011, 1, 1, 9, 1))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 15, 0))
      end

      it 'wraps to next day after hour 15' do
        result = cycle.next(Time.gm(2011, 1, 1, 15, 1))
        expect(result[0]).to eq(Time.gm(2011, 1, 2, 3, 0))
      end
    end

    describe '#previous' do
      it 'returns hour 15 from hour 16' do
        result = cycle.previous(Time.gm(2011, 1, 1, 16, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 15, 0))
      end

      it 'returns hour 15 when called from hour 15' do
        result = cycle.previous(Time.gm(2011, 1, 1, 15, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 15, 0))
      end

      it 'returns hour 9 from hour 14' do
        result = cycle.previous(Time.gm(2011, 1, 1, 14, 0))
        expect(result[0]).to eq(Time.gm(2011, 1, 1, 9, 0))
      end
    end

    describe '#member?' do
      it 'excludes hour 0' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 0, 0))).to be false
      end

      it 'includes hour 3' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 3, 0))).to be true
      end

      it 'excludes hour 6' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 6, 0))).to be false
      end

      it 'includes hour 9' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 9, 0))).to be true
      end

      it 'excludes hour 12' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 12, 0))).to be false
      end

      it 'includes hour 15' do
        expect(cycle.member?(Time.gm(2011, 1, 1, 15, 0))).to be true
      end
    end

    describe '#first' do
      it 'returns hour 3 as first valid hour' do
        expect(cycle.first).to eq(Time.gm(2011, 1, 1, 3, 0))
      end
    end
  end

  describe 'validation' do
    it 'raises error when both exclude_hours and valid_hours are specified' do
      expect do
        described_class.new('201101010000 201101012300 01:00:00', 'test', 0, nil, '6 18', '3 9 15')
      end.to raise_error(RuntimeError)
    end

    it 'raises error for invalid hour value > 23 in exclude_hours' do
      expect do
        described_class.new('201101010000 201101012300 01:00:00', 'test', 0, nil, '25')
      end.to raise_error(RuntimeError)
    end

    it 'raises error for invalid hour value > 23 in valid_hours' do
      expect do
        described_class.new('201101010000 201101012300 01:00:00', 'test', 0, nil, nil, '30')
      end.to raise_error(RuntimeError)
    end

    it 'raises error for negative hour value' do
      expect do
        described_class.new('201101010000 201101012300 01:00:00', 'test', 0, nil, '-1')
      end.to raise_error(RuntimeError)
    end
  end
end
