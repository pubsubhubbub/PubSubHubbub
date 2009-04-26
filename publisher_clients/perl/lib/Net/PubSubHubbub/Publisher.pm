package Net::PubSubHubbub::Publisher;
use strict;
use LWP::UserAgent;
use HTTP::Request::Common;
use Carp qw(croak);

our $VERSION = "1.00";

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
