=head1 NAME

TheSchwartz::Worker::PubSubHubbubPublish - ping pubsubhubbub hub servers

=head1 SYNOPSIS

  use TheSchwartz;
  use TheSchwartz::Worker::PubSubHubbubPublish;
  my $sclient = TheSchwartz->new(databases => \@Conf::YOUR_DBS);
  $sclient->can_do("TheSchwartz::Worker::PubSubHubbubPublish");
  $sclient->work; # main loop of program; goes forever, pinging as needed

=head1 DESCRIPTION

This is a worker class for sending pings to PubSubHubbub hub servers.
See L<TheSchwartz> and L<Net::PubSubHubbub::Publisher> for more
information.

=head1 JOB ARGUMENTS

When constructing a job using L<TheSchwartz>'s insert_job
method, construct your L<TheSchwartz::Job> instance with its
'argument' of the following form:

   {
      hub => $hub_url,   # the hub's endpoint URL
      topic_url => $url, # Atom URL that was updated 
   }

Also, if you set your L<TheSchwartz::Job>'s C<coalesce> property to be
the hub URL, this worker will do batch pings instead, vastly reducing
the number of HTTP requests it does.

=cut

package TheSchwartz::Worker::PubSubHubbubPublish;
use strict;
use base 'TheSchwartz::Worker';
use Storable;
use Net::PubSubHubbub::Publisher 0.91;

our $VERSION = '1.00';

our $MAX_BATCH_SIZE = 50;

my $keep_exit_status_for = 0;
sub set_keep_exit_status_for { $keep_exit_status_for = shift; }

my %publisher;  # $hub -> Net::PubSubHubbub::Publisher

sub work {
    my ($class, $job) = @_;
    my $client = $job->handle->client;
    my $hub    = $job->arg->{hub};
    unless ($hub && $hub =~ m!^https?://\S+$!) {
        $job->permanent_failure("Bogus hub $hub. Ignoring job.");
        return;
    }

    my @jobs;
    my @topics;

    my $add_job = sub {
        my $j = shift;
        my $args = $j->arg;
        unless ($args->{hub} eq $hub) {
            # Each job must share the same hub.
            warn "WARNING: coalesced job had different hub in its args. Skipping.";
            return;
        }

        push @jobs, $j;
        push @topics, $args->{topic_url};
    };
    $add_job->($job);

    my $publisher = $publisher{$hub} ||=
        Net::PubSubHubbub::Publisher->new(hub => $hub);

    while (@topics < $MAX_BATCH_SIZE) {
        my $j = $client->find_job_with_coalescing_value(__PACKAGE__, $hub);
        last unless $j;
        $add_job->($j);
    }

    if ($publisher->publish_update(@topics)) {
        warn "Pinged $hub about topic(s): @topics.\n";
        foreach my $j (@jobs) {
            $j->completed;
        }
        return;
    }

    my $failure_reason = $publisher->last_response->status_line;
    warn "Failed to ping $hub about @topics: $failure_reason\n";
    $job->failed($failure_reason);
}

sub keep_exit_status_for {
    return 0 unless $keep_exit_status_for;
    return $keep_exit_status_for->() if ref $keep_exit_status_for eq "CODE";
    return $keep_exit_status_for;
}

sub grab_for { 30 }
sub max_retries { 10 }
sub retry_delay {
    my ($class, $fails) = @_;
    return 30 * $fails;
}

=head1 AUTHOR

Brad Fitzpatrick -- brad@danga.com

=head1 COPYRIGHT, LICENSE, and WARRANTY

Copyright 2009, Brad Fitzpatrick.

License to use under the same terms as Perl itself.

This software comes with no warranty of any kind.

=head1 SEE ALSO

L<TheSchwartz>

L<http://code.google.com/p/pubsubhubbub/>

=cut

1;
