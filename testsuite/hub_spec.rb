require 'hub'
require 'mocks'
require 'timeout'

HUB_URL = ENV['HUB_URL']
raise "Specify a hub URL by setting the HUB_URL environment variable." unless HUB_URL
HUB_PUBLISH_URL = "#{HUB_URL}#{ENV['PUBLISH_PATH'] || '/publish'}"
HUB_SUBSCRIBE_URL = "#{HUB_URL}#{ENV['SUBSCRIBE_PATH'] || '/subscribe'}"

def wait_on(something)
  Timeout::timeout(3) { break unless something.nil? while true }
end

def as_optional
  # TODO: record as optional spec failure
end

shared_examples_for "a hub with publisher and subscriber" do
  before(:all) do
    @hub = Hub.new(HUB_PUBLISH_URL, HUB_SUBSCRIBE_URL)
    @publisher = Publisher.new(@hub)
    @subscriber = Subscriber.new(@hub)
  end
  
  after(:all) do
    @publisher.stop
    @subscriber.stop
  end
end

describe Hub, "publisher interface" do
  it_should_behave_like "a hub with publisher and subscriber"
  
  it "accepts POST request for publish notifications" do
    @hub.publish(@publisher.content_url).should be_a_kind_of(Net::HTTPSuccess)
  end
  
  it "SHOULD arrange for a content fetch request after publish notification" # shouldn't it always?
  
  it "MUST return 202 Accepted if publish notification was accepted" do
    @hub.publish(@publisher.content_url).should be_an_instance_of(Net::HTTPAccepted)
  end
  
  it "MUST return appropriate HTTP error response code if not accepted" do
    @hub.post_as_publisher(nil, nil)          .should be_a_kind_of(Net::HTTPClientError)
    @hub.post_as_publisher('not publish', nil).should be_a_kind_of(Net::HTTPClientError)
  end
  
  it "sends an HTTP GET request to the topic URL to fetch content" do
    # Because GAE-PSH doesn't fetch content unless there are subscriptions, we subscribe
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN)
    
    request_method = nil
    @publisher.onrequest = lambda {|req| request_method = req.request_method }
    @hub.publish(@publisher.content_url)
    wait_on request_method
    request_method.should == "GET"
  end
  
  it "SHOULD include a header field X-Hub-Subscribers whose value is an integer in content fetch request" do
    # Because GAE-PSH doesn't fetch content unless there are subscriptions, we subscribe
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN)
    
    headers = nil
    @publisher.onrequest = lambda {|req| headers = req.header }
    @hub.publish(@publisher.content_url)
    wait_on headers
    headers.should include("X-Hub-Subscribers") rescue as_optional
  end
  
  
end

describe Hub, "subscriber interface" do
  it_should_behave_like "a hub with publisher and subscriber"
  
  it "accepts POST request for subscription requests" do
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN).should be_a_kind_of(Net::HTTPSuccess)
  end
  
  it "REQUIRES mode, callback, topic, and verify parameters in the subscription request" do 
    @hub.post_as_subscriber(nil, @subscriber.callback_url, @publisher.content_url, nil, Subscriber::VERIFY_TOKEN).should_not be_a_kind_of(Net::HTTPSuccess)
    @hub.subscribe(nil, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN).should_not be_a_kind_of(Net::HTTPSuccess)
    @hub.subscribe(@subscriber.callback_url, nil, 'sync', Subscriber::VERIFY_TOKEN).should_not be_a_kind_of(Net::HTTPSuccess)
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, nil, Subscriber::VERIFY_TOKEN).should_not be_a_kind_of(Net::HTTPSuccess)
  end
  
  it "MUST ignore verify keywords it does not understand" do
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync,foobar,async', Subscriber::VERIFY_TOKEN).should be_a_kind_of(Net::HTTPSuccess)
  end
  
  it "MUST return 204 No Content if subscription was created and verified" do
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN).should be_a_kind_of(Net::HTTPNoContent)
  end
  
  it "MUST return 202 Accepted if the subscription was created but has yet to be verified" do
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'async', Subscriber::VERIFY_TOKEN).should be_a_kind_of(Net::HTTPAccepted)
  end
  
  it "MUST return appropriate HTTP error response code in case of any error" do
    @hub.post_as_subscriber(nil, nil, nil, nil)             .should be_a_kind_of(Net::HTTPClientError)
    @hub.post_as_subscriber('not subscribe', nil, nil, nil) .should be_a_kind_of(Net::HTTPClientError)
  end
  
  it "SHOULD return a description of an error in the response body in plain text"
  
  it "MUST complete verification before returning a response in synchronous mode"
  
  it "must verify subscriber with a GET request to the callback URL" do
    request_method = nil
    @subscriber.onrequest = lambda {|req| request_method = req.request_method }
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN)
    wait_on request_method
    request_method.should == "GET"
  end
  
  it "is REQUIRED to include mode, topic and challenge query parameters in the verification request" do
    query_string = nil
    @subscriber.onrequest = lambda {|req| query_string = req.query_string }
    @hub.subscribe(@subscriber.callback_url, @publisher.content_url, 'sync', Subscriber::VERIFY_TOKEN)
    wait_on query_string
    query_string.should include("hub.mode=")
    query_string.should include("hub.topic=")
    query_string.should include("hub.challenge=")
  end
  
  it "expects an HTTP success code with the challenge parameter in the response body to verify subscription"
  
  it "expects subscriber to return 404 Not Found if they do not agree with the subscription"
  
  it "MUST consider other client and server response codes to mean subscription is not verified"
  
  it "SHOULD retry verification until a definite acknowledgement is received"

end
