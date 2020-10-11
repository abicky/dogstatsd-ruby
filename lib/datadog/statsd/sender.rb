# frozen_string_literal: true

module Datadog
  class Statsd
    class Sender
      CLOSEABLE_QUEUES = Queue.instance_methods.include?(:close)

      def initialize(message_buffer, buffer_flush_interval: nil)
        @message_buffer = message_buffer
        @buffer_flush_interval = buffer_flush_interval
        @mutex = Mutex.new
        @cv = ConditionVariable.new
        @state = :stopped
      end

      def flush(sync: false)
        raise ArgumentError, 'Start sender first' if @state == :stopped

        message_queue.push(:flush)

        rendez_vous if sync
      end

      def rendez_vous
        # Initialize and get the thread's sync queue
        queue = (Thread.current[:statsd_sync_queue] ||= Queue.new)
        # tell sender-thread to notify us in the current
        # thread's queue
        message_queue.push(queue)
        # wait for the sender thread to send a message
        # once the flush is done
        queue.pop
      end

      def add(message)
        raise ArgumentError, 'Start sender first' if @state == :stopped

        message_queue << message
      end

      def start
        raise ArgumentError, 'Sender already started' if @state == :running

        # initialize message queue for background thread
        @message_queue = Queue.new
        @state = :running
        # start background thread
        @sender_thread = Thread.new(&method(:send_loop))
        @flush_thread = Thread.new(&method(:flush_loop)) if @buffer_flush_interval
      end

      if CLOSEABLE_QUEUES
        def stop(join_worker: true)
          @state = :stopping
          @mutex.synchronize { @cv.broadcast }
          flush_thread.join if flush_thread

          @state = :stopped
          message_queue.close if message_queue
          sender_thread.join if sender_thread && join_worker
        end
      else
        def stop(join_worker: true)
          @state = :stopping
          @mutex.synchronize { @cv.broadcast }
          flush_thread.join if flush_thread

          @state = :stopped
          message_queue << :close if message_queue
          sender_thread.join if sender_thread && join_worker
        end
      end

      private

      attr_reader :message_buffer

      attr_reader :message_queue
      attr_reader :sender_thread
      attr_reader :flush_thread

      if CLOSEABLE_QUEUES
        def send_loop
          until (message = message_queue.pop).nil? && message_queue.closed?
            # skip if message is nil, e.g. when message_queue
            # is empty and closed
            next unless message

            case message
            when :flush
              message_buffer.flush
            when Queue
              message.push(:go_on)
            else
              message_buffer.add(message)
            end
          end

          @message_queue = nil
          @sender_thread = nil
        end
      else
        def send_loop
          loop do
            message = message_queue.pop

            next unless message

            case message
            when :close
              break
            when :flush
              message_buffer.flush
            when Queue
              message.push(:go_on)
            else
              message_buffer.add(message)
            end
          end

          @message_queue = nil
          @sender_thread = nil
        end
      end

      def flush_loop
        last_flush_time = current_time
        while @state == :running
          @mutex.synchronize do
            @cv.wait(@mutex, @buffer_flush_interval - (current_time - last_flush_time))
            last_flush_time = current_time
            flush(sync: @state == :running)
          end
        end

        @flush_thread = nil
      end

      if Process.const_defined?(:CLOCK_MONOTONIC)
        def current_time
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
      else
        def current_time
          Time.now
        end
      end
    end
  end
end
