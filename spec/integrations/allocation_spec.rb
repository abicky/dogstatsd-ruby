# frozen_string_literal: true

require 'spec_helper'

describe 'Allocations and garbage collection' do
  before do
    skip 'Ruby too old' if RUBY_VERSION < '2.3.0'
  end

  let(:socket) { FakeUDPSocket.new }

  subject do
    Datadog::Statsd.new('localhost', 1234,
      namespace: namespace,
      sample_rate: sample_rate,
      tags: tags,
      logger: logger,
      telemetry_flush_interval: -1,
    )
  end

  let(:namespace) { 'sample_ns' }
  let(:sample_rate) { nil }
  let(:tags) { %w[abc def] }
  let(:logger) do
    Logger.new(log).tap do |logger|
      logger.level = Logger::INFO
    end
  end
  let(:log) { StringIO.new }

  before do
    allow(Socket).to receive(:new).and_return(socket)
    allow(UDPSocket).to receive(:new).and_return(socket)
    # initializing statsd so it does not count in allocations
  end

  context 'sending increments' do
    before do
      # warmup
      subject.increment('foobar', tags: { something: 'a value' })
      subject.flush
    end

    let(:expected_allocations) do
      if RUBY_VERSION < '2.4.0'
        17
      elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
        16
      else
        15
      end
    end

    it 'produces low amounts of garbage' do
      expect do
        subject.increment('foobar')
        subject.flush
      end.to make_allocations(expected_allocations)
    end

    context 'without telemetry' do
      subject do
        Datadog::Statsd.new('localhost', 1234,
          namespace: namespace,
          sample_rate: sample_rate,
          tags: tags,
          logger: logger,
          telemetry_enable: false,
        )
      end

      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          9
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          8
        else
          7
        end
      end

      it 'produces even lower amounts of garbage' do
        expect do
          subject.increment('foobar')
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end

    context 'with tags' do
      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          25
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          24
        else
          23
        end
      end

      it 'produces low amounts of garbage' do
        expect do
          subject.increment('foobar', tags: { something: 'a value' }) { 1111 }
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end
  end

  context 'sending timings' do
    before do
      # warmup
      subject.time('foobar', tags: { something: 'a value' }) { 1111 }
      subject.flush
    end

    let(:expected_allocations) do
      if RUBY_VERSION < '2.4.0'
        17
      elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
        16
      else
        15
      end
    end

    it 'produces low amounts of garbage' do
      expect do
        subject.time('foobar') { 1111 }
        subject.flush
      end.to make_allocations(expected_allocations)
    end

    context 'without telemetry' do
      subject do
        Datadog::Statsd.new('localhost', 1234,
          namespace: namespace,
          sample_rate: sample_rate,
          tags: tags,
          logger: logger,
          telemetry_enable: false,
        )
      end

      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          9
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          8
        else
          7
        end
      end

      it 'produces even lower amounts of garbage' do
        expect do
          subject.time('foobar') { 1111 }
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end

    context 'with tags' do
      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          25
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          24
        else
          23
        end
      end

      it 'produces low amounts of garbage' do
        expect do
          subject.time('foobar', tags: { something: 'a value' }) { 1111 }
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end
  end

  context 'sending events' do
    before do
      # warmup
      subject.event('foobar', 'happening', tags: { something: 'a value' })
      subject.flush
    end

    let(:expected_allocations) do
      if RUBY_VERSION < '2.4.0'
        19
      elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
        18
      else
        17
      end
    end

    it 'produces low amounts of garbage' do
      expect do
        subject.event('foobar', 'happening')
        subject.flush
      end.to make_allocations(expected_allocations)
    end

    context 'without telemetry' do
      subject do
        Datadog::Statsd.new('localhost', 1234,
          namespace: namespace,
          sample_rate: sample_rate,
          tags: tags,
          logger: logger,
          telemetry_enable: false,
        )
      end

      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          11
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          10
        else
          9
        end
      end

      it 'produces even lower amounts of garbage' do
        expect do
          subject.event('foobar', 'happening')
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end

    context 'with tags' do
      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          27
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          26
        else
          25
        end
      end

      it 'produces low amounts of garbage' do
        expect do
          subject.event('foobar', 'happening', tags: { something: 'a value' })
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end
  end

  context 'sending service checks' do
    before do
      # warmup
      subject.service_check('foobar', 'happening', tags: { something: 'a value' })
      subject.flush
    end

    let(:expected_allocations) do
      if RUBY_VERSION < '2.4.0'
        15
      elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
        14
      else
        13
      end
    end

    it 'produces low amounts of garbage' do
      expect do
        subject.service_check('foobar', 'ok')
        subject.flush
      end.to make_allocations(expected_allocations)
    end

    context 'without telemetry' do
      subject do
        Datadog::Statsd.new('localhost', 1234,
          namespace: namespace,
          sample_rate: sample_rate,
          tags: tags,
          logger: logger,
          telemetry_enable: false,
        )
      end

      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          7
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          6
        else
          5
        end
      end

      it 'produces even lower amounts of garbage' do
        expect do
          subject.service_check('foobar', 'ok')
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end

    context 'with tags' do
      let(:expected_allocations) do
        if RUBY_VERSION < '2.4.0'
          23
        elsif RUBY_VERSION >= '2.4.0' && RUBY_VERSION < '2.5.0'
          22
        else
          21
        end
      end

      it 'produces low amounts of garbage' do
        expect do
          subject.service_check('foobar', 'ok', tags: { something: 'a value' })
          subject.flush
        end.to make_allocations(expected_allocations)
      end
    end
  end
end