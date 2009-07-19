require 'webrick'

class Subscriber
  PORT = 8089
  VERIFY_TOKEN = 'qfwef9'
  
  attr_reader :callback_url
  attr_accessor :onrequest
  
  def initialize(hub)
    @hub = hub
    @server = WEBrick::HTTPServer.new(:Port => PORT, :Logger => WEBrick::Log.new(nil, 0), :AccessLog => WEBrick::Log.new(nil, 0))
    @callback_url = "http://localhost:#{PORT}/callback"
    @onrequest = lambda {|req|}
    mount "/callback" do |req,res|
      @onrequest.call(req)
      res.status = 200
      if req.request_method == 'GET'
        res.body = /hub.challenge=([^$|&]+)/.match(req.query_string)[1]
      else

      end
    end
    @server_thread = Thread.new do 
      trap("INT"){ @server.shutdown }
      @server.start
    end
  end
  
  def mount(path, &block)
    @server.mount(path, WEBrick::HTTPServlet::ProcHandler.new(block))
  end
  
  def stop
    @server.shutdown
    @server_thread.join
  end
end


class Publisher
  PORT = 8088
  
  attr_reader :content_url
  attr_reader :content
  attr_accessor :onrequest
  
  attr_accessor :last_request_method
  attr_accessor :last_headers
  
  def initialize(hub)
    @hub = hub
    @server = WEBrick::HTTPServer.new(:Port => PORT, :Logger => WEBrick::Log.new(nil, 0), :AccessLog => WEBrick::Log.new(nil, 0))
    @content_url = "http://localhost:#{PORT}/happycats.xml"
    @last_request_method = nil
    @last_headers = nil
    @onrequest = lambda {|req|}
    @content =<<EOF
<?xml version="1.0"?>
<feed>
  <!-- Normally here would be source, title, etc ... -->

  <link rel="hub" href="#{@hub.endpoint}" />
  <link rel="self" href="#{@content_url}" />
  <updated>2008-08-11T02:15:01Z</updated>

  <!-- Example of a full entry. -->
  <entry>
    <title>Heathcliff</title>
    <link href="http://publisher.example.com/happycat25.xml" />
    <id>http://publisher.example.com/happycat25.xml</id>
    <updated>2008-08-11T02:15:01Z</updated>
    <content>
      What a happy cat. Full content goes here.
    </content>
  </entry>

  <!-- Example of an entity that isn't full/is truncated. This is implied
       by the lack of a <content> element and a <summary> element instead. -->
  <entry >
    <title>Heathcliff</title>
    <link href="http://publisher.example.com/happycat25.xml" />
    <id>http://publisher.example.com/happycat25.xml</id>
    <updated>2008-08-11T02:15:01Z</updated>
    <summary>
      What a happy cat!
    </summary>
  </entry>

  <!-- Meta-data only; implied by the lack of <content> and
       <summary> elements. -->
  <entry>
    <title>Garfield</title>
    <link rel="alternate" href="http://publisher.example.com/happycat24.xml" />
    <id>http://publisher.example.com/happycat25.xml</id>
    <updated>2008-08-11T02:15:01Z</updated>
  </entry>

  <!-- Context entry that's meta-data only and not new. Implied because the
       update time on this entry is before the //atom:feed/updated time. -->
  <entry>
    <title>Nermal</title>
    <link rel="alternate" href="http://publisher.example.com/happycat23s.xml" />
    <id>http://publisher.example.com/happycat25.xml</id>
    <updated>2008-07-10T12:28:13Z</updated>
  </entry>

</feed>
EOF
    mount "/happycats.xml" do |req,res|
      @onrequest.call(req)
      @last_request_method = req.request_method
      @last_headers = req.header
      res.status = 200
      res['Content-Type'] = 'application/atom+xml'
      res.body = @content
    end
    @server_thread = Thread.new do 
      trap("INT"){ @server.shutdown }
      @server.start
    end
  end
  
  def mount(path, &block)
    @server.mount(path, WEBrick::HTTPServlet::ProcHandler.new(block))
  end
  
  def stop
    @server.shutdown
    @server_thread.join
  end
end
