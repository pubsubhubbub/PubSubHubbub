#!/usr/bin/perl

use strict;
use lib 'lib';
use TheSchwartz;
use TheSchwartz::Worker::PubSubHubbubPublish;

my $client = TheSchwartz->new(databases => [{
    user => "root",
    dsn => "dbi:mysql:theschwartz",
}]);

$client->can_do("TheSchwartz::Worker::PubSubHubbubPublish");
$client->work;
