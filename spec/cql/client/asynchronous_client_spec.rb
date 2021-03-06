# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe AsynchronousClient do
      let :client do
        described_class.new(connection_options)
      end

      let :connection_options do
        {:host => 'lvho.st', :port => 12321, :io_reactor => io_reactor}
      end

      let :io_reactor do
        FakeIoReactor.new
      end

      def connections
        io_reactor.connections
      end

      def last_connection
        connections.last
      end

      def requests
        last_connection.requests
      end

      def last_request
        requests.last
      end

      def handle_request(&handler)
        @request_handler = handler
      end

      before do
        io_reactor.on_connection do |connection|
          connection.handle_request do |request, _, _|
            response = nil
            if @request_handler
              response = @request_handler.call(request, connection, proc { connection.default_request_handler(request) })
            end
            unless response
              response = connection.default_request_handler(request)
            end
            response
          end
        end
      end

      describe '#connect' do
        it 'connects' do
          client.connect.get
          connections.should have(1).item
        end

        it 'connects only once' do
          client.connect.get
          client.connect.get
          connections.should have(1).item
        end

        context 'when connecting to multiple hosts' do
          before do
            client.close.get
            io_reactor.stop.get
          end

          it 'connects to all hosts' do
            c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h3.lvho.st]))
            c.connect.get
            connections.should have(3).items
          end

          it 'connects to all hosts, when given as a comma-sepatated string' do
            c = described_class.new(connection_options.merge(:host => 'h1.lvho.st,h2.lvho.st,h3.lvho.st'))
            c.connect.get
            connections.should have(3).items
          end

          it 'only connects to each host once' do
            c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h2.lvho.st]))
            c.connect.get
            connections.should have(2).items
          end

          it 'succeeds even if only one of the connections succeeded' do
            io_reactor.node_down('h1.lvho.st')
            io_reactor.node_down('h3.lvho.st')
            c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h2.lvho.st]))
            c.connect.get
            connections.should have(1).items
          end

          it 'fails when all nodes are down' do
            io_reactor.node_down('h1.lvho.st')
            io_reactor.node_down('h2.lvho.st')
            io_reactor.node_down('h3.lvho.st')
            c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h2.lvho.st]))
            expect { c.connect.get }.to raise_error(Io::ConnectionError)
          end
        end

        it 'returns itself' do
          client.connect.get.should equal(client)
        end

        it 'connects to the right host and port' do
          client.connect.get
          last_connection.host.should == 'lvho.st'
          last_connection.port.should == 12321
        end

        it 'connects with the default connection timeout' do
          client.connect.get
          last_connection.timeout.should == 10
        end

        it 'sends a startup request' do
          client.connect.get
          requests.first.should be_a(Protocol::StartupRequest)
        end

        it 'sends a startup request to each connection' do
          client.close.get
          io_reactor.stop.get
          io_reactor.start.get

          c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h3.lvho.st]))
          c.connect.get
          connections.each do |cc|
            cc.requests.first.should be_a(Protocol::StartupRequest)
          end
        end

        it 'is not in a keyspace' do
          client.connect.get
          client.keyspace.should be_nil
        end

        it 'changes to the keyspace given as an option' do
          c = described_class.new(connection_options.merge(:keyspace => 'hello_world'))
          c.connect.get
          request = requests.find { |rq| rq == Protocol::QueryRequest.new('USE hello_world', :one) }
          request.should_not be_nil, 'expected a USE request to have been sent'
        end

        it 'validates the keyspace name before sending the USE command' do
          c = described_class.new(connection_options.merge(:keyspace => 'system; DROP KEYSPACE system'))
          expect { c.connect.get }.to raise_error(InvalidKeyspaceNameError)
          requests.should_not include(Protocol::QueryRequest.new('USE system; DROP KEYSPACE system', :one))
        end

        context 'with automatic peer discovery' do
          let :local_info do
            {
              'data_center' => 'dc1',
              'host_id' => nil,
            }
          end

          let :local_metadata do
            [
              ['system', 'local', 'data_center', :text],
              ['system', 'local', 'host_id', :uuid],
            ]
          end

          let :peer_metadata do
            [
              ['system', 'peers', 'peer', :inet],
              ['system', 'peers', 'data_center', :varchar],
              ['system', 'peers', 'host_id', :uuid],
              ['system', 'peers', 'rpc_address', :inet],
            ]
          end

          let :data_centers do
            Hash.new('dc1')
          end

          let :additional_nodes do
            Array.new(5) { IPAddr.new("127.0.#{rand(255)}.#{rand(255)}") }
          end

          before do
            uuid_generator = TimeUuid::Generator.new
            additional_rpc_addresses = additional_nodes.dup
            io_reactor.on_connection do |connection|
              connection[:spec_host_id] = uuid_generator.next
              connection[:spec_data_center] = data_centers[connection.host]
              connection.handle_request do |request, _, _|
                case request
                when Protocol::StartupRequest
                  Protocol::ReadyResponse.new
                when Protocol::QueryRequest
                  case request.cql
                  when /FROM system\.local/
                    row = {'host_id' => connection[:spec_host_id], 'data_center' => connection[:spec_data_center]}
                    Protocol::RowsResultResponse.new([row], local_metadata)
                  when /FROM system\.peers/
                    other_host_ids = connections.reject { |c| c[:spec_host_id] == connection[:spec_host_id] }.map { |c| c[:spec_host_id] }
                    until other_host_ids.size >= 2
                      other_host_ids << uuid_generator.next
                    end
                    rows = other_host_ids.map do |host_id|
                      ip = additional_rpc_addresses.shift
                      {'host_id' => host_id, 'data_center' => data_centers[ip], 'rpc_address' => ip}
                    end
                    Protocol::RowsResultResponse.new(rows, peer_metadata)
                  end
                end
              end
            end
          end

          it 'connects to the other nodes in the cluster' do
            client.connect.get
            connections.should have(3).items
          end

          it 'connects to the other nodes in the same data center' do
            data_centers[additional_nodes[1]] = 'dc2'
            client.connect.get
            connections.should have(2).items
          end

          it 'connects to the other nodes in same data centers as the seed nodes' do
            data_centers['host2'] = 'dc2'
            data_centers[additional_nodes[1]] = 'dc2'
            c = described_class.new(connection_options.merge(:hosts => %w[host1 host2]))
            c.connect.get
            connections.should have(3).items
          end

          it 'only connects to the other nodes in the cluster it is not already connected do' do
            c = described_class.new(connection_options.merge(:hosts => %w[host1 host2]))
            c.connect.get
            connections.should have(3).items
          end

          it 'handles the case when it is already connected to all nodes' do
            c = described_class.new(connection_options.merge(:hosts => %w[host1 host2 host3 host4]))
            c.connect.get
            connections.should have(4).items
          end

          it 'accepts that some nodes are down' do
            io_reactor.node_down(additional_nodes.first.to_s)
            client.connect.get
            connections.should have(2).items
          end
        end

        it 're-raises any errors raised' do
          io_reactor.stub(:connect).and_raise(ArgumentError)
          expect { client.connect.get }.to raise_error(ArgumentError)
        end

        it 'is not connected if an error is raised' do
          io_reactor.stub(:connect).and_raise(ArgumentError)
          client.connect.get rescue nil
          client.should_not be_connected
          io_reactor.should_not be_running
        end

        it 'is connected after #connect returns' do
          client.connect.get
          client.should be_connected
        end

        it 'is not connected while connecting' do
          go = false
          io_reactor.stop.get
          io_reactor.before_startup { sleep 0.01 until go }
          client.connect
          begin
            client.should_not be_connected
          ensure
            go = true
          end
        end

        context 'when the server requests authentication' do
          def accepting_request_handler(request, *args)
            case request
            when Protocol::StartupRequest
              Protocol::AuthenticateResponse.new('com.example.Auth')
            when Protocol::CredentialsRequest
              Protocol::ReadyResponse.new
            end
          end

          def denying_request_handler(request, *args)
            case request
            when Protocol::StartupRequest
              Protocol::AuthenticateResponse.new('com.example.Auth')
            when Protocol::CredentialsRequest
              Protocol::ErrorResponse.new(256, 'No way, José')
            end
          end

          before do
            handle_request(&method(:accepting_request_handler))
          end

          it 'sends credentials' do
            client = described_class.new(connection_options.merge(:credentials => {'username' => 'foo', 'password' => 'bar'}))
            client.connect.get
            request = requests.find { |rq| rq == Protocol::CredentialsRequest.new('username' => 'foo', 'password' => 'bar') }
            request.should_not be_nil, 'expected a credentials request to have been sent'
          end

          it 'raises an error when no credentials have been given' do
            client = described_class.new(connection_options)
            expect { client.connect.get }.to raise_error(AuthenticationError)
          end

          it 'raises an error when the server responds with an error to the credentials request' do
            handle_request(&method(:denying_request_handler))
            client = described_class.new(connection_options.merge(:credentials => {'username' => 'foo', 'password' => 'bar'}))
            expect { client.connect.get }.to raise_error(AuthenticationError)
          end

          it 'shuts down the client when there is an authentication error' do
            handle_request(&method(:denying_request_handler))
            client = described_class.new(connection_options.merge(:credentials => {'username' => 'foo', 'password' => 'bar'}))
            client.connect.get rescue nil
            client.should_not be_connected
            io_reactor.should_not be_running
          end
        end
      end

      describe '#close' do
        it 'closes the connection' do
          client.connect.get
          client.close.get
          io_reactor.should_not be_running
        end

        it 'does nothing when called before #connect' do
          client.close.get
        end

        it 'accepts multiple calls to #close' do
          client.connect.get
          client.close.get
          client.close.get
        end

        it 'returns itself' do
          client.connect.get.close.get.should equal(client)
        end

        it 'fails when the IO reactor stop fails' do
          io_reactor.stub(:stop).and_return(Future.failed(StandardError.new('Bork!')))
          expect { client.close.get }.to raise_error('Bork!')
        end
      end

      describe '#use' do
        it 'executes a USE query' do
          handle_request do |request, _, _|
            if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE system'
              Protocol::SetKeyspaceResultResponse.new('system')
            end
          end
          client.connect.get
          client.use('system').get
          last_request.should == Protocol::QueryRequest.new('USE system', :one)
        end

        it 'executes a USE query for each connection' do
          client.close.get
          io_reactor.stop.get
          io_reactor.start.get

          c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h3.lvho.st]))
          c.connect.get

          c.use('system').get
          last_requests = connections.select { |c| c.host =~ /^h\d\.example\.com$/ }.sort_by(&:host).map { |c| c.requests.last }
          last_requests.should == [
            Protocol::QueryRequest.new('USE system', :one),
            Protocol::QueryRequest.new('USE system', :one),
            Protocol::QueryRequest.new('USE system', :one)
          ]
        end

        it 'knows which keyspace it changed to' do
          handle_request do |request, _, _|
            if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE system'
              Protocol::SetKeyspaceResultResponse.new('system')
            end
          end
          client.connect.get
          client.use('system').get
          client.keyspace.should == 'system'
        end

        it 'raises an error if the keyspace name is not valid' do
          client.connect.get
          expect { client.use('system; DROP KEYSPACE system').get }.to raise_error(InvalidKeyspaceNameError)
        end

        it 'allows the keyspace name to be quoted' do
          handle_request do |request, _, _|
            if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE "system"'
              Protocol::SetKeyspaceResultResponse.new('system')
            end
          end
          client.connect.get
          client.use('"system"').get
          client.keyspace.should == "system"
        end
      end

      describe '#execute' do
        before do
          client.connect.get
        end

        it 'asks the connection to execute the query' do
          client.execute('UPDATE stuff SET thing = 1 WHERE id = 3').get
          last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :quorum)
        end

        it 'uses the specified consistency' do
          client.execute('UPDATE stuff SET thing = 1 WHERE id = 3', :three).get
          last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :three)
        end

        context 'with a void CQL query' do
          it 'returns nil' do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql =~ /UPDATE/
                Protocol::VoidResultResponse.new
              end
            end
            result = client.execute('UPDATE stuff SET thing = 1 WHERE id = 3').get
            result.should be_nil
          end
        end

        context 'with a USE query' do
          it 'returns nil' do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE system'
                Protocol::SetKeyspaceResultResponse.new('system')
              end
            end
            result = client.execute('USE system').get
            result.should be_nil
          end

          it 'knows which keyspace it changed to' do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE system'
                Protocol::SetKeyspaceResultResponse.new('system')
              end
            end
            client.execute('USE system').get
            client.keyspace.should == 'system'
          end

          it 'detects that one connection changed to a keyspace and changes the others too' do
            client.close.get
            io_reactor.stop.get
            io_reactor.start.get

            handle_request do |request, connection, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql == 'USE system'
                Protocol::SetKeyspaceResultResponse.new('system')
              end
            end

            c = described_class.new(connection_options.merge(:hosts => %w[h1.lvho.st h2.lvho.st h3.lvho.st]))
            c.connect.get

            c.execute('USE system', :one).get
            c.keyspace.should == 'system'

            last_requests = connections.select { |c| c.host =~ /^h\d\.example\.com$/ }.sort_by(&:host).map { |c| c.requests.last }
            last_requests.should == [
              Protocol::QueryRequest.new('USE system', :one),
              Protocol::QueryRequest.new('USE system', :one),
              Protocol::QueryRequest.new('USE system', :one)
            ]
          end
        end

        context 'with an SELECT query' do
          let :rows do
            [['xyz', 'abc'], ['abc', 'xyz'], ['123', 'xyz']]
          end

          let :metadata do
            [['thingies', 'things', 'thing', :text], ['thingies', 'things', 'item', :text]]
          end

          let :result do
            client.execute('SELECT * FROM things').get
          end

          before do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql =~ /FROM things/
                Protocol::RowsResultResponse.new(rows, metadata)
              end
            end
          end

          it 'returns an Enumerable of rows' do
            row_count = 0
            result.each do |row|
              row_count += 1
            end
            row_count.should == 3
          end

          context 'with metadata that' do
            it 'has keyspace, table and type information' do
              result.metadata['item'].keyspace.should == 'thingies'
              result.metadata['item'].table.should == 'things'
              result.metadata['item'].column_name.should == 'item'
              result.metadata['item'].type.should == :text
            end

            it 'is an Enumerable' do
              result.metadata.map(&:type).should == [:text, :text]
            end

            it 'is splattable' do
              ks, table, col, type = result.metadata['thing']
              ks.should == 'thingies'
              table.should == 'things'
              col.should == 'thing'
              type.should == :text
            end
          end
        end

        context 'when there is an error creating the request' do
          it 'returns a failed future' do
            f = client.execute('SELECT * FROM stuff', :foo)
            expect { f.get }.to raise_error(ArgumentError)
          end
        end

        context 'when the response is an error' do
          before do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::QueryRequest) && request.cql =~ /FROM things/
                Protocol::ErrorResponse.new(0xabcd, 'Blurgh')
              end
            end
          end

          it 'raises an error' do
            expect { client.execute('SELECT * FROM things').get }.to raise_error(QueryError, 'Blurgh')
          end

          it 'decorates the error with the CQL that caused it' do
            begin
              client.execute('SELECT * FROM things').get
            rescue QueryError => e
              e.cql.should == 'SELECT * FROM things'
            else
              fail('No error was raised')
            end
          end
        end
      end

      describe '#prepare' do
        let :id do
          'A' * 32
        end

        let :metadata do
          [['stuff', 'things', 'item', :varchar]]
        end

        before do
          handle_request do |request, _, _|
            if request.is_a?(Protocol::PrepareRequest)
              Protocol::PreparedResultResponse.new(id, metadata)
            end
          end
        end

        before do
          client.connect.get
        end

        it 'sends a prepare request' do
          client.prepare('SELECT * FROM system.peers').get
          last_request.should == Protocol::PrepareRequest.new('SELECT * FROM system.peers')
        end

        it 'returns a prepared statement' do
          statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?').get
          statement.should_not be_nil
        end

        it 'executes a prepared statement' do
          statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?').get
          statement.execute('foo').get
          last_request.should == Protocol::ExecuteRequest.new(id, metadata, ['foo'], :quorum)
        end

        it 'returns a prepared statement that knows the metadata' do
          statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?').get
          statement.metadata['item'].type == :varchar
        end

        it 'executes a prepared statement with a specific consistency level' do
          statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?').get
          statement.execute('thing', :local_quorum).get
          last_request.should == Protocol::ExecuteRequest.new(id, metadata, ['thing'], :local_quorum)
        end

        context 'when there is an error creating the request' do
          it 'returns a failed future' do
            f = client.prepare(nil)
            expect { f.get }.to raise_error(ArgumentError)
          end
        end

        context 'when there is an error preparing the request' do
          it 'returns a failed future' do
            handle_request do |request, _, _|
              if request.is_a?(Protocol::PrepareRequest)
                Protocol::PreparedResultResponse.new(id, metadata)
              end
            end
            statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?').get
            f = statement.execute
            expect { f.get }.to raise_error(ArgumentError)
          end
        end
      end

      context 'when not connected' do
        it 'is not connected before #connect has been called' do
          client.should_not be_connected
        end

        it 'is not connected after #close has been called' do
          client.connect.get
          client.close.get
          client.should_not be_connected
        end

        it 'complains when #use is called before #connect' do
          expect { client.use('system').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #use is called after #close' do
          client.connect.get
          client.close.get
          expect { client.use('system').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #execute is called before #connect' do
          expect { client.execute('DELETE FROM stuff WHERE id = 3').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #execute is called after #close' do
          client.connect.get
          client.close.get
          expect { client.execute('DELETE FROM stuff WHERE id = 3').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #prepare is called before #connect' do
          expect { client.prepare('DELETE FROM stuff WHERE id = 3').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #prepare is called after #close' do
          client.connect.get
          client.close.get
          expect { client.prepare('DELETE FROM stuff WHERE id = 3').get }.to raise_error(NotConnectedError)
        end

        it 'complains when #execute of a prepared statement is called after #close' do
          handle_request do |request, _, _|
            if request.is_a?(Protocol::PrepareRequest)
              Protocol::PreparedResultResponse.new('A' * 32, [])
            end
          end
          client.connect.get
          statement = client.prepare('DELETE FROM stuff WHERE id = 3').get
          client.close.get
          expect { statement.execute.get }.to raise_error(NotConnectedError)
        end
      end
    end
  end
end
