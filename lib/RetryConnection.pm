
use AnyEvent::Gearman::Connection;

my $meta = AnyEvent::Gearman::Connection;
$meta->

=pod
undef(*AnyEvent::Gearman::Connection::connect);
*AnyEvent::Gearman::Connection::connect = sub{
    my ($self) = @_;
 
    # already connected
    return if $self->handler;
print "PORT:".$self->_port."\n"; 
    my $g = tcp_connect $self->_host, $self->_port, sub {
        my ($fh) = @_;
 
        if ($fh) {
            my $handle = AnyEvent::Handle->new(
                fh       => $fh,
                on_read  => sub { $self->process_packet },
                on_error => sub {
                    my @undone = @{ $self->_need_handle },
                                 values %{ $self->_job_handles };
                    $_->event('on_fail') for @undone;
 
                    $self->_need_handle([]);
                    $self->_job_handles({});
                    $self->mark_dead;
                },
            );
            $self->handler( $handle );
            $_->() for map { $_->[0] } @{ $self->on_connect_callbacks };
        }
        else {
            warn sprintf("Connection failed: %s", $!);
            $self->mark_dead;
            $_->() for map { $_->[1] } @{ $self->on_connect_callbacks };
        }
 
        $self->on_connect_callbacks( [] );
    };
 
    weaken $self;
    $self->_con_guard($g);
 
    $self;
};
=cut
1;
