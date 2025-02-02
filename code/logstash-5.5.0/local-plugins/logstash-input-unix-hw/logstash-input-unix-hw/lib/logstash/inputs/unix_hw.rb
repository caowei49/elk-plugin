# encoding: utf-8
require "json"
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/socket_peer"

# Read events over a UNIX socket.
#
# Like `stdin` and `file` inputs, each event is assumed to be one line of text.
#
# Can either accept connections from clients or connect to a server,
# depending on `mode`.
class LogStash::Inputs::Unix_hw < LogStash::Inputs::Base
  class Interrupted < StandardError;
  end
  config_name "unix_hw"

  default :codec, "plain"

  # When mode is `server`, the path to listen on.
  # When mode is `client`, the path to connect to.
  config :path, :validate => :string, :required => true

  # Remove socket file in case of EADDRINUSE failure
  config :force_unlink, :validate => :boolean, :default => false

  # The 'read' timeout in seconds. If a particular connection is idle for
  # more than this timeout period, we will assume it is dead and close it.
  #
  # If you never want to timeout, use -1.
  config :data_timeout, :validate => :number, :default => -1

  # Mode to operate in. `server` listens for client connections,
  # `client` connects to a server.
  config :mode, :validate => ["server", "client"], :default => "server"

  # Amount of time in seconds to wait if the socket file is not present, before retrying.
  # Only positive values are allowed.
  #
  # This setting is only used if `mode` is `client`.
  config :socket_not_present_retry_interval_seconds, :validate => :number, :required => true, :default => 5

  def initialize(*args)
    super(*args)
  end

  # def initialize

  public

  def register
    require "socket"
    require "timeout"
    require 'logger'
    require "protocol_buffers"

    # Write error info into file
    log_file = File.open('./logs/telemetry-gpb.log', 'a')
    @file_logger = Logger.new(log_file)
    @file_logger.level = Logger::ERROR
    #
    if server?
      @logger.info("Starting unix input listener", :address => "#{@path}", :force_unlink => "#{@force_unlink}")
      begin
        @server_socket = UNIXServer.new(@path)
      rescue Errno::EADDRINUSE, IOError
        if @force_unlink
          File.unlink(@path)
          begin
            @server_socket = UNIXServer.new(@path)
            return
          rescue Errno::EADDRINUSE, IOError
            msg = "!!!Could not start UNIX server: Address in use"
            @logger.error("!!!Could not start UNIX server: Address in use",
                          :path => @path)
            @file_logger.error(:msg => msg,
                               :path => @path)
            raise
          end
        end
        msg = "!!!Could not start UNIX server: Address in use"
        @logger.error("!!!Could not start UNIX server: Address in use",
                      :path => @path)
        @file_logger.error(:msg => msg,
                           :path => @path)
        raise
      end
    else # client
      if @socket_not_present_retry_interval_seconds < 0
        @logger.warn("Value #{@socket_not_present_retry_interval_seconds} for socket_not_present_retry_interval_seconds is not valid, using default value of 5 instead")
        @socket_not_present_retry_interval_seconds = 5
      end
    end
  end

  # def register

  private

  def handle_socket(socket, output_queue)
    begin
      hostname = Socket.gethostname
      while !stop?
        buf = nil
        # NOTE(petef): the timeout only hits after the line is read
        # or socket dies
        # TODO(sissel): Why do we have a timeout here? What's the point?
        if @data_timeout == -1
          len = socket.readpartial(4).unpack("N")
          encoding = socket.readpartial(2).unpack("n")
          @logger.info("encoding: "+encoding.inspect+"len "+len.inspect)
          @logger.info("encoding[0]: #{encoding[0]}")
          buf = socket.readpartial(len[0])
        else
          Timeout::timeout(@data_timeout) do
            buf = socket.readpartial(16384)
          end
        end
        # gpb编码
        if (encoding[0] == 0)
          @codec.decode(buf) do |event|
            decorate(event)
            event.set("host", hostname) unless event.include?("host")
            event.set("path", @path) unless event.include?("path")
            output_queue << event
          end
        else
          jsondata = JSON.parse(buf)
          jsondata[:company] = "huawei"
          event = LogStash::Event.new(jsondata)
          @logger.debug("jsondata: "+jsondata.to_s)
          output_queue << event
        end

      end
    rescue => e
      @logger.error("Closing connection", :path => @path, :exception => e, :backtrace => e.backtrace)
    rescue Timeout::Error
      @logger.error("Closing connection after read timeout", :path => @path)
    end # begin

  ensure
    # begin
    #   socket.close
    # rescue IOError
    #   #pass
    # end
  end

  private

  def server?
    @mode == "server"
  end

  # def server?

  public

  def run(output_queue)
    if server?

      @client_threads = []
      while !stop?
        # Start a new thread for each connection.
        @client_threads << Thread.start(@server_socket.accept) do |s|
          @logger.debug("Accepted connection", :server => "#{@path}")
          handle_socket(s, output_queue)
        end
      end
    else
      while !stop?
        if File.socket?(@path) then
          @client_socket = UNIXSocket.new(@path)
          @client_socket.instance_eval {
            class << self;
              include ::LogStash::Util::SocketPeer
            end }
          @logger.info("Opened connection", :client => @path)
          handle_socket(@client_socket, output_queue)
        else
          @logger.warn("Socket not present, wait for #{@subscription_retry_interval_seconds} seconds for socket to appear", :client => @path)
          sleep @socket_not_present_retry_interval_seconds
        end
      end
    end
  rescue IOError
    # if stop is called during @server_socket.accept
    # the thread running `run` will raise an IOError
    # We catch IOError here and do nothing, just let the method terminate
  end

  # def run

  public

  def stop
    if server?
      File.unlink(@path)
      @server_socket.close unless @server_socket.nil?
    else
      @client_socket.close unless @client_socket.nil?
    end
  rescue IOError
    # if socket with @mode == client was closed by the client, an other call to @client_socket.close
    # will raise an IOError. We catch IOError here and do nothing, just let logstash terminate
    @logger.warn("Cloud not close socket while Logstash is shutting down. Socket already closed by the other party?", :path => @path)
  end # def stop
end # class LogStash::Inputs::Unix_hw
