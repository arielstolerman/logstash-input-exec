# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "open3"
require "socket" # for Socket.gethostname

require "stud/interval"

# Periodically run a shell command and capture the whole output as an event.
#
# Notes:
#
# * The `command` field of this event will be the command run.
# * The `message` field of this event will be the entire stdout of the command.
#
class LogStash::Inputs::Exec < LogStash::Inputs::Base

  config_name "exec"

  default :codec, "plain"

  # Command to run. For example, `uptime`
  config :command, :validate => :string, :required => true

  # Interval to run the command. Value is in seconds.
  config :interval, :validate => :number, :required => true
  
  # Toggle using legacy executor, using IO instead of Open3
  config :legacy_execute, :validate => :boolean, :default => false

  # Toggle whether to log executed command stderr to INFO.
  # Only applies when legacy_execute is false.
  config :log_stderr, :validate => :boolean, :default => true

  def register
    @logger.info("Registering Exec Input", :type => @type, :command => @command, :interval => @interval)
    @hostname = Socket.gethostname
    @io       = nil
  end # def register

  def run(queue)
    while !stop?
      inner_run(queue)
    end # loop
  end # def run

  def inner_run(queue)
    start = Time.now
    if @legacy_execute
      legacy_execute(@command, queue)
    else
      execute(@command, queue)
    end
    duration = Time.now - start

    @logger.debug? && @logger.debug("Command completed", :command => @command, :duration => duration)

    wait_until_end_of_interval(duration)
  end

  private

  # Wait until the end of the interval
  # @param [Integer] the duration of the last command executed
  def wait_until_end_of_interval(duration)
    # Sleep for the remainder of the interval, or 0 if the duration ran
    # longer than the interval.
    sleeptime = [0, @interval - duration].max
    if sleeptime > 0
      Stud.stoppable_sleep(sleeptime) { stop? }
    else
      @logger.warn("Execution ran longer than the interval. Skipping sleep.",
      :command => @command, :duration => duration, :interval => @interval)
    end
  end
  
  def emit(event, queue)
    decorate(event)
    event.set("host", @hostname)
    event.set("command", command)
    queue << event
  end

  def stop
    # TODO
    return
  end

  # Execute a given command
  # @param [String] A command string
  # @param [Array or Queue] A queue to append events to
  def execute(command, queue)
    begin
      i, o, e, t = Open3.popen3(command)
      @logger.info("Running exec", :command => command, :pid => t[:pid])
      { :out => o, :err => e }.each do |k, s|
        Thread.new do
          until (line = s.gets).nil? do
            if k == :out
              @codec.decode(line) do |event|
                emit(event, queue)
              end
            elsif @log_stderr
              @logger.info(line)
            end
          end
        end
      end
      # wait for the process to finish
      t.join
      i.close
      o.close
      e.close
    rescue Exception => e
      @logger.error("Exception while running command",
        :command => command, :e => e, :backtrace => e.backtrace)
    ensure
      stop
    end
  end
  
  def legacy_stop
    return if @io.nil? || @io.closed?
    @io.close
    @io = nil
  end

  # Execute a given command
  # @param [String] A command string
  # @param [Array or Queue] A queue to append events to
  def legacy_execute(command, queue)
    @logger.debug? && @logger.debug("Running exec", :command => command)
    begin
      @io = IO.popen(command)
      @codec.decode(@io.read) do |event|
        emit(event, queue)
      end
    rescue StandardError => e
      @logger.error("Error while running command",
        :command => command, :e => e, :backtrace => e.backtrace)
    rescue Exception => e
      @logger.error("Exception while running command",
        :command => command, :e => e, :backtrace => e.backtrace)
    ensure
      legacy_stop
    end
  end

end # class LogStash::Inputs::Exec
