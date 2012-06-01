package IPC::AnyEvent::Gearman;
# ABSTRACT: IPC through gearmand.
use namespace::autoclean;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($ERROR);
use Any::Moose;
use Data::Dumper;
use AnyEvent::Gearman;
use AnyEvent::Gearman::Worker::RetryConnection;
use UUID::Random;

# VERSION


=attr job_servers

ArrayRef of hosts. *REQUIRED*

=cut
has 'job_servers' => (is => 'rw', isa => 'ArrayRef',required => 1);

=attr channel

get/set channel. When set, reconnect to new channel.
It is set with Random-UUID by default.

=cut

has channel => (is => 'rw', default=>sub{UUID::Random::generate;} );

=attr on_recv

on_recv Hander.
First argument is DATA which is sent.
This can be invoked after listen().

=cut
has 'on_recv' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'You need to set on_recv function'};}
);
=attr on_sent

on_sent handler.
First argument is a channel string.

=cut
has 'on_sent' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{INFO 'Send OK '.$_[0]};}
);
=attr on_fail

on_fail handler.
First argument is a channel string.

=cut
has 'on_fail' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'Send FAIL '.$_[0]};}
);

has 'client' => (is=>'rw', lazy=>1, isa=>'Object',
default=>sub{
    DEBUG 'lazy client';
    my $self = shift;
    return gearman_client @{$self->job_servers()};
},
);

has 'worker' => (is=>'rw', isa=>'Object',);

after 'channel' => sub{
    my $self = shift;
    if( @_ && $self->{listening}){
        $self->_renew_connection();    
    }
};

after 'job_servers' => sub{
    my $self = shift;
    if( @_ && $self->{listening}){
        $self->_renew_connection();    
    }
    if( @_ ){
        $self->client( gearman_client @{$self->job_servers()} );
    }
};

=method listen

To receive message, you MUST call listen().

    my $sender = IPC::AnyEvent::Gearman->new(channel=>'ADMIN',job_servers=>['localhost:9998']);
    $sender->listen();

=cut
sub listen{
    my $self = shift;
    $self->{listening} = 1;
    $self->_renew_connection();
}

=method send

To send data to process listening channel, use this.

    my $sender = IPC::AnyEvent::Gearman->new(job_servers=>['localhost:9998']);
    $sender->send($channel,'DATA');

=cut
sub send{
    my $self = shift;
    my $target_channel= shift;
    my $data = shift;
    $self->client->add_task(
        $target_channel => $data,
        on_complete => sub{
            my $result = $_[1];
            $self->on_sent()->($target_channel,$_[1]);
        },
        on_fail => sub{
            $self->on_fail()->($target_channel);
        }
    );
}

sub _renew_connection{
    my $self = shift;
    DEBUG "new Connection";
    my $worker = gearman_worker @{$self->job_servers()};
    $worker = AnyEvent::Gearman::Worker::RetryConnection::patch_worker($worker);
    $self->worker( $worker );
    $self->worker->register_function(
        $self->channel() => sub{
            my $job = shift;
            my $res = $self->on_recv()->($job->workload);
            $res = '' unless defined($res);
            $job->complete($res);
        }
    );
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=pod


=head1 SYNOPSIS

    use AnyEvent;
    use IPC::AnyEvent::Gearman;
    
    #receive    
    my $recv = IPC::AnyEvent::Gearman->new(job_servers=>['localhost:9999']);
    $recv->channel('BE_CALLED'); # channel is set with a random UUID by default 
    $recv->on_recv(sub{
        my $msg = shift;
        print "received msg : $data\n";
        return "OK";#result
    });
    $recv->listen();

    my $cv = AE::cv;
    $cv->recv;

    #send
    my $ch = 'BE_CALLED';
    my $send = IPC::AnyEvent::Gearman->new(server=>['localhost:9999']);
    my $result = $send->send($ch,"TEST DATA");
    pritn $result; # prints "OK"
    
=cut

