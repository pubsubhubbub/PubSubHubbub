#!/usr/bin/perl

use strict;
use TheSchwartz;

my $client = TheSchwartz->new(databases => [{
    user => "root",
    dsn => "dbi:mysql:theschwartz",
}]);

my $hub_url = "http://pubsubhubbub.appspot.com/publish";

my $topic = "http://publisher.example.com/topic/" . rand() . ".atom";
print "Submitting dummy topic $topic ...\n";

my $job = TheSchwartz::Job->new(funcname => 'TheSchwartz::Worker::PubSubHubbubPublish',
                                arg => {
                                    hub => $hub_url,
                                    topic_url => $topic,
                                },
                                coalesce => $hub_url,
                                );

my $handle = $client->insert($job);
print "  job handle: $handle\n";
