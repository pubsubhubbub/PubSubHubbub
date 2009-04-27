use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'TheSchwartz::Worker::PubSubHubbubPublish',
               'VERSION_FROM' => 'lib/TheSchwartz/Worker/PubSubHubbubPublish.pm',
               'PREREQ_PM'    => {
                   'TheSchwartz::Worker' => 0,
                   'Net::PubSubHubbub::Publisher' => 0.91,
               },
               ABSTRACT_FROM => 'lib/TheSchwartz/Worker/PubSubHubbubPublish.pm',
               AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>',
               );



