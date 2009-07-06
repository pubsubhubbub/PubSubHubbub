#!/usr/bin/perl

use strict;
use TheSchwartz;
use Getopt::Long;

my $n_jobs = 1;
my $hub_url = "http://pubsubhubbub.appspot.com/";

GetOptions("n=i"   => \$n_jobs,
           "hub=s" => \$hub_url,
           ) or die "Unknown options.";

my $client = TheSchwartz->new(databases => [{
    user => "root",
    dsn => "dbi:mysql:theschwartz",
}]);

for (1..$n_jobs) {
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
}

