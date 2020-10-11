require 'spec_helper'

describe Datadog::Statsd::Sender do
  subject do
    described_class.new(message_buffer, buffer_flush_interval: buffer_flush_interval)
  end

  let(:message_buffer) do
    instance_double(Datadog::Statsd::MessageBuffer)
  end
  let(:buffer_flush_interval) { nil }

  describe '#start' do
    after do
      subject.stop
    end

    it 'starts a worker thread' do
      expect do
        subject.start
      end.to change { Thread.list.size }.by(1)
    end

    context 'when the sender is started' do
      before do
        subject.start
      end

      it 'raises an ArgumentError' do
        expect do
          subject.start
        end.to raise_error(ArgumentError, /Sender already started/)
      end
    end

    context 'when buffer_flush_interval is set' do
      let(:buffer_flush_interval) { 0.1 }

      it 'starts a worker thread and a flush thread' do
        expect do
          subject.start
        end.to change { Thread.list.size }.by(2)
      end
    end
  end

  describe '#stop' do
    before do
      subject.start
    end

    it 'stops the worker thread' do
      expect do
        subject.stop
      end.to change { Thread.list.size }.by(-1)
    end

    context 'when buffer_flush_interval is set' do
      let(:buffer_flush_interval) { 0.1 }

      it 'stops the worker thread and the flush thread' do
        mutex = Mutex.new
        cv = ConditionVariable.new
        expect(subject).to receive(:flush).with(sync: true) do
          mutex.synchronize { cv.broadcast }
        end
        # Wait until the flush thread starts
        mutex.synchronize { cv.wait(mutex, 1) }

        expect(subject).to receive(:flush).with(sync: false)
        expect do
          subject.stop
        end.to change { Thread.list.size }.by(-2)
      end
    end
  end

  describe '#add' do
    context 'when the sender is not started' do
      it 'raises an ArgumentError' do
        expect do
          subject.add('sample message')
        end.to raise_error(ArgumentError, /Start sender first/)
      end
    end

    context 'when starting and stopping' do
      before do
        subject.start
      end

      after do
        subject.stop
      end

      it 'adds a message to the message buffer asynchronously (needs rendez_vous)' do
        expect(message_buffer)
          .to receive(:add)
          .with('sample message')

        subject.add('sample message')

        subject.rendez_vous
      end
    end
  end

  describe '#flush' do
    context 'when the sender is not started' do
      it 'raises an ArgumentError' do
        expect do
          subject.add('sample message')
        end.to raise_error(ArgumentError, /Start sender first/)
      end
    end

    context 'when starting and stopping' do
      before do
        subject.start
      end

      after do
        subject.stop
      end

      context 'without sync mode' do
        it 'flushes the message buffer (needs rendez_vous)' do
          expect(message_buffer)
            .to receive(:flush)

          subject.flush
          subject.rendez_vous
        end
      end

      context 'with sync mode' do
        it 'flushes the message buffer and waits (no explicit rendez_vous)' do
          expect(message_buffer)
            .to receive(:flush)

          subject.flush(sync: true)
        end
      end
    end
  end
end
