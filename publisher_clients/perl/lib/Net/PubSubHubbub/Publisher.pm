package Net::PubSubHubbub::Publisher;
use strict;
use LWP::UserAgent;
use HTTP::Request::Common;
use Carp qw(croak);

=head1 NAME

Net::PubSubHubbub::Publisher - client library to ping a PubSubHubbub hub

=head1 OVERVIEW

  my $publisher = Net::PubSubHubbub::Publisher->new(hub => $hub);
  $publisher->publish_update($atom_topic_url);

=cut

our $VERSION = "0.90";

=head1 CONSTRUCTOR

=over 4

=item C<new>(hub => $hub[, ua => $ua])

Takes a required hub URL, and an optional L<LWP::UserAgent> instance.

=back

=cut

sub new {
    my ($class, %opts) = @_;
    my $ua = delete $opts{ua};
    my $hub = delete $opts{hub};
    unless ($hub) {
        croak("Required option 'hub' not set.");
    }
    unless ($hub =~ m!^https?://!) {
        croak("Bogus hub URL of $hub");
    }
    if (%opts) {
        die "Unknown options: " . join(", ", sort keys %opts);
    }
    unless ($ua) {
        $ua = LWP::UserAgent->new(
                                  keep_alive => 1,
                                  agent => "Net-PubSubHubbub-Publisher-perl/$VERSION",
                                  );
    }
    return bless {
        ua => $ua,
        hub => $hub,
    }, $class;
}

=head1 METHODS

=over 4

=item C<publish_update>($topic_url)

Sends a ping that the provided Topic URL has been updated.

=back

=cut

sub publish_update {
    my ($self, $url) = @_;
    unless ($url =~ m!^https?://!) {
        croak("Bogus URL of $url");
    }
    my $req = POST $self->{hub}, [
                                  "hub.mode" => "publish",
                                  "hub.url" => $url,
                                  ];
    my $res = $self->{last_res} = $self->{ua}->request($req);
    return 1 if $res->is_success;
    return 0;
}

sub last_response {
    my $self = shift;
    return $self->{last_res};
}

1;

=head1 SEE ALSO

L<http://code.google.com/p/pubsubhubbub/> -- PubSubHubbub home

=head1 AUTHOR

Brad Fitzpatrick <brad@danga.com>

=cut
