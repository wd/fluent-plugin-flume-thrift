module Fluent

  class FlumeOutput < BufferedOutput
    Fluent::Plugin.register_output('flume', self)

    config_param :host,      :string,  :default => 'localhost'
    config_param :port,      :integer, :default => 35863
    config_param :timeout,   :integer, :default => 30
    config_param :connect_timeout,   :integer, :default => 30
    attr_reader :nodes

    def initialize
      require 'thrift'
      $:.unshift File.join(File.dirname(__FILE__), 'thrift')
      require 'thrift_source_protocol'
      require 'flume_types'

      @nodes = []  #=> [Node]
      @localhost_name = Socket.gethostbyname(Socket.gethostname).first #your reverse dns setup should be ok

      super
    end

    def configure(conf)
      super

      conf.elements.each do |e|
        next if e.name != "server"

        host = e['host']
        port = e['port']
        port = port ? port.to_i : DEFAULT_LISTEN_PORT

        name = e['name']
        unless name
          name = "#{host}:#{port}"
        end

        @nodes << RawNode.new(name, host, port)
        $log.info "Adding forwarding server '#{name}'", :host=>host, :port=>port
      end
    end

    def format(tag,time,record)
       [tag, time, record].to_msgpack
    end

    def write(chunk)
      return if chunk.empty?

      error = nil

      @nodes.each do |node|
        begin
          send_data(node.client, chunk)
          return
        rescue
          error = $!
          $log.error "Got error for node '#{node.name}': #{error}, try reconnect"
          node.connect()
        end
      end

      raise error if error
      raise "No nodes available, check your conf file!"
    end

    private
    def send_data(client, chunk)
      count = 0
      chunk.msgpack_each {|(tag,time,record)|
        msg = {
            'body' => record.to_json.to_s.force_encoding('UTF-8'),
            'headers' => {
              'timestamp' => (time*1000).to_s,
              'hostname' => @localhost_name,
              'host' => @localhost_name,
              'tag' => tag
            }
        }
        $log.debug "Send msg: #{msg}"
        entry = ThriftFlumeEvent.new(msg)
        client.append entry
        count += 1
      }
      $log.debug "Writing #{count} entries to flume"
    end

    class RawNode
      attr_reader :name, :host, :port, :client

      def initialize(name, host, port)
        @name = name
        @host = host
        @port = port
        begin
          connect()
        rescue
          $log.error("Connect thrift server failed: #!")
        end
      end

      def connect()
        Timeout.timeout(@connect_timeout) do
          socket = Thrift::Socket.new(@host, @port, @timeout)
          transport = Thrift::FramedTransport.new(socket)
          protocol = Thrift::CompactProtocol.new(transport)
          @client = ThriftSourceProtocol::Client.new(protocol)
          transport.open()
          $log.debug "Thrift client for '#{@name}' connected"
        end
      end
    end
  end
end
