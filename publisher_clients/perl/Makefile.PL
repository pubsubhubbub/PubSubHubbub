#!/usr/bin/perl
use strict;
use ExtUtils::MakeMaker;

WriteMakefile( NAME            => 'Net::PubSubHubbub::Publisher',
               VERSION_FROM    => 'lib/Net/PubSubHubbub/Publisher.pm',
               PREREQ_PM       => {
                   'LWP::UserAgent' => 0,
               },
               ABSTRACT_FROM => 'lib/Net/PubSubHubbub/Publisher.pm',
               AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>',
               );




